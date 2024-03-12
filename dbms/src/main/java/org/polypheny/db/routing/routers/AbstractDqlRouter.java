/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.routing.routers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.core.lpg.LpgAlg.NodeType;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyString;


/**
 * Abstract router for DQL routing.
 * It will create alg builders and select column placements per table.
 * There are three abstract methods:
 * handleHorizontalPartitioning
 * handleVerticalPartitioning
 * handleNonePartitioning
 * Every table will be check by the following order:
 * first if partitioned horizontally {@code ->} handleHorizontalPartitioning called.
 * second if partitioned vertically or replicated {@code ->} handleVerticalPartitioningOrReplication
 * third, all other cases {@code ->} handleNonePartitioning
 */
@Slf4j
public abstract class AbstractDqlRouter extends BaseRouter implements Router {

    /**
     * Boolean which cancel routing plan generation.
     *
     * In Universal Routing, not every router needs to propose a plan for every query. But, every router will be asked to try
     * to prepare a plan for the query. If a router sees, that he is not able to route (maybe after tablePlacement 2 out of 4,
     * the router is expected to return an empty list of proposed routing plans. To "abort" routing plan creation, the boolean
     * cancelQuery is introduced and will be checked during creation. As we are in the middle of traversing a AlgNode, this
     * is the new to "abort" traversing the AlgNode.
     */
    protected boolean cancelQuery = false;


    /**
     * Abstract methods which will determine routing strategy. Not implemented in abstract class.
     * If implementing router is not supporting one of the three methods, empty list can be returned and cancelQuery boolean set to false.
     */
    protected abstract List<RoutedAlgBuilder> handleHorizontalPartitioning(
            AlgNode node,
            LogicalTable table,
            List<RoutedAlgBuilder> builders,
            RoutingContext context );

    protected abstract List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication(
            AlgNode node,
            LogicalTable table,
            List<RoutedAlgBuilder> builders,
            RoutingContext context );

    protected abstract List<RoutedAlgBuilder> handleNonePartitioning(
            AlgNode node,
            LogicalTable table,
            List<RoutedAlgBuilder> builders,
            RoutingContext context );


    /**
     * Abstract router only routes DQL queries.
     */
    @Override
    public List<RoutedAlgBuilder> route( AlgRoot logicalRoot, RoutingContext context ) {
        // Reset cancel query this run
        this.cancelQuery = false;

        if ( logicalRoot.alg instanceof LogicalRelModify ) {
            throw new IllegalStateException( "Should never happen for DML" );
        } else if ( logicalRoot.alg instanceof ConditionalExecute ) {
            throw new IllegalStateException( "Should never happen for conditional executes" );
        } else if ( logicalRoot.alg instanceof BatchIterator ) {
            throw new IllegalStateException( "Should never happen for Iterator" );
        } else {
            RoutedAlgBuilder builder = context.getRoutedAlgBuilder();
            return buildDql(
                    logicalRoot.alg,
                    Lists.newArrayList( builder ),
                    context );
        }
    }


    @Override
    public <T extends AlgNode & LpgAlg> AlgNode routeGraph( RoutedAlgBuilder builder, T alg, Statement statement ) {
        if ( alg.getInputs().size() == 1 ) {
            routeGraph( builder, (AlgNode & LpgAlg) alg.getInput( 0 ), statement );
            if ( builder.stackSize() > 0 ) {
                alg.replaceInput( 0, builder.build() );
            }
            return alg;
        } else if ( alg.getNodeType() == NodeType.SCAN ) {

            builder.push( handleGraphScan( (LogicalLpgScan) alg, statement, null, null ) );
            return alg;
        } else if ( alg.getNodeType() == NodeType.VALUES ) {
            return alg;
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public void resetCaches() {
        joinedScanCache.invalidateAll();
    }


    protected List<RoutedAlgBuilder> buildDql( AlgNode node, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        if ( node instanceof SetOp ) {
            if ( node instanceof Union ) {
                // unions can have more than one child
                return buildUnion( node, builders, context );
            } else {
                return buildSetOp( node, builders, context );
            }
        } else {
            return buildSelect( node, builders, context );
        }
    }


    protected List<RoutedAlgBuilder> buildSelect( AlgNode node, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builders = this.buildDql( node.getInput( i ), builders, context );
        }

        if ( node instanceof LogicalDocumentScan ) {
            return Lists.newArrayList( builders.get( 0 ).push( super.handleDocScan( (DocumentScan<?>) node, context.getStatement(), null ) ) );
        }

        if ( node.unwrap( LogicalRelScan.class ).isPresent() && node.getEntity() != null ) {
            Optional<LogicalEntity> oLogicalEntity = node.getEntity().unwrap( LogicalEntity.class );

            if ( oLogicalEntity.isEmpty() ) {
                throw new GenericRuntimeException( "Unexpected table. Only logical tables expected here!" );
            }

            if ( oLogicalEntity.get().getDataModel() == DataModel.GRAPH ) {
                return handleRelationalOnGraphScan( node, oLogicalEntity.get(), builders, context );
            }

            if ( oLogicalEntity.get().getDataModel() == DataModel.DOCUMENT ) {
                builders.forEach( b -> {
                    LogicalRelScan scan = new LogicalRelScan(
                            context.getCluster(),
                            node.getTraitSet(),
                            handleDocScan(
                                    new LogicalDocumentScan( context.getCluster(), node.getTraitSet(), oLogicalEntity.get() ), context.getStatement(), List.of() ).entity );
                    b.push( scan );
                } );
                return builders;
            }

            Optional<LogicalTable> oLogicalTable = oLogicalEntity.get().unwrap( LogicalTable.class );

            if ( oLogicalTable.isEmpty() ) {
                throw new GenericRuntimeException( "Unexpected table. Only logical tables expected here!" );
            }

            // Check if table is even horizontal partitioned

            if ( catalog.getSnapshot().alloc().getPartitionsFromLogical( oLogicalTable.get().id ).size() > 1 ) {
                return handleHorizontalPartitioning( node, oLogicalTable.get(), builders, context );
            } else if ( catalog.getSnapshot().alloc().getPlacementsFromLogical( oLogicalTable.get().id ).size() > 1 ) { // At the moment multiple strategies
                return handleVerticalPartitioningOrReplication( node, oLogicalTable.get(), builders, context );
            }

            return handleNonePartitioning( node, oLogicalTable.get(), builders, context );

        } else if ( node instanceof LogicalRelValues ) {
            return Lists.newArrayList( super.handleValues( (LogicalRelValues) node, builders ) );
        } else {
            return Lists.newArrayList( super.handleGeneric( node, builders ) );
        }
    }


    private List<RoutedAlgBuilder> handleRelationalOnGraphScan( AlgNode node, LogicalEntity logicalTable, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        AlgBuilder algBuilder = AlgBuilder.create( context.getStatement() );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();

        algBuilder.lpgScan( logicalTable.id );
        algBuilder.lpgMatch( List.of( algBuilder.lpgNodeMatch( List.of( PolyString.of( logicalTable.name ) ) ) ), List.of( PolyString.of( "n" ) ) );
        algBuilder.lpgProject(
                List.of( rexBuilder.makeToJson( rexBuilder.makeLpgGetId() ), rexBuilder.makeToJson( rexBuilder.makeLpgPropertiesExtract() ), rexBuilder.makeLpgLabels() ),
                List.of( PolyString.of( "id" ), PolyString.of( "properties" ), PolyString.of( "labels" ) ) );

        AlgNode built = routeGraph( context.getRoutedAlgBuilder(), (AlgNode & LpgAlg) algBuilder.build(), context.getStatement() );

        builders.get( 0 ).push( new LogicalTransformer(
                node.getCluster(),
                node.getTraitSet().replace( ModelTrait.RELATIONAL ),
                List.of( built ),
                null,
                ModelTrait.GRAPH,
                ModelTrait.RELATIONAL,
                GraphType.ofRelational(),
                true ) );
        return builders;
    }


    protected List<RoutedAlgBuilder> buildSetOp( AlgNode node, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, context );

        RoutedAlgBuilder builder0 = context.getRoutedAlgBuilder();
        RoutedAlgBuilder b0 = buildDql( node.getInput( 1 ), Lists.newArrayList( builder0 ), context ).get( 0 );

        builders.forEach(
                builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek(), b0.build() ) ) )
        );

        return builders;
    }


    protected List<RoutedAlgBuilder> buildUnion( AlgNode node, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, context );

        RoutedAlgBuilder builder0 = context.getRoutedAlgBuilder();
        List<RoutedAlgBuilder> b0s = new ArrayList<>();
        for ( AlgNode input : node.getInputs().subList( 1, node.getInputs().size() ) ) {
            b0s.add( buildDql( input, Lists.newArrayList( builder0 ), context ).get( 0 ) );
        }

        builders.forEach(
                builder -> builder.replaceTop( node.copy(
                        node.getTraitSet(),
                        ImmutableList.copyOf(
                                Stream.concat(
                                                Stream.of( builder.peek() ),
                                                b0s.stream().map( AlgBuilder::build ) )
                                        .toList() ) ) )
        );

        return builders;
    }

}
