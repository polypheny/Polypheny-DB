/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentAlg.DocType;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.core.lpg.LpgAlg.NodeType;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;


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
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation );

    protected abstract List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation );

    protected abstract List<RoutedAlgBuilder> handleNonePartitioning(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation );


    /**
     * Abstract router only routes DQL queries.
     */
    @Override
    public List<RoutedAlgBuilder> route( AlgRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation ) {
        // Reset cancel query this run
        this.cancelQuery = false;

        if ( logicalRoot.alg instanceof LogicalModify ) {
            throw new IllegalStateException( "Should never happen for DML" );
        } else if ( logicalRoot.alg instanceof ConditionalExecute ) {
            throw new IllegalStateException( "Should never happen for conditional executes" );
        } else if ( logicalRoot.alg instanceof BatchIterator ) {
            throw new IllegalStateException( "Should never happen for Iterator" );
        } else {
            RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
            List<RoutedAlgBuilder> routedAlgBuilders = buildDql(
                    logicalRoot.alg,
                    Lists.newArrayList( builder ),
                    statement,
                    logicalRoot.alg.getCluster(),
                    queryInformation );
            return routedAlgBuilders;
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

            builder.push( handleGraphScan( (LogicalLpgScan) alg, statement, null ) );
            return alg;
        } else if ( alg.getNodeType() == NodeType.VALUES ) {
            return alg;
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public <T extends AlgNode & DocumentAlg> AlgNode routeDocument( RoutedAlgBuilder builder, T alg, Statement statement ) {
        if ( alg.getInputs().size() == 1 ) {
            routeDocument( builder, (AlgNode & DocumentAlg) alg.getInput( 0 ), statement );
            if ( builder.stackSize() > 0 ) {
                alg.replaceInput( 0, builder.build() );
            }
            return alg;
        } else if ( alg.getDocType() == DocType.SCAN ) {
            builder.push( handleDocumentScan( (DocumentScan) alg, statement, builder, null ).build() );
            return alg;
        } else if ( alg.getDocType() == DocType.VALUES ) {
            return alg;
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public void resetCaches() {
        joinedScanCache.invalidateAll();
    }


    protected List<RoutedAlgBuilder> buildDql( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( node instanceof SetOp ) {
            if ( node instanceof Union ) {
                // unions can have more than one child
                return buildUnion( node, builders, statement, cluster, queryInformation );
            } else {
                return buildSetOp( node, builders, statement, cluster, queryInformation );
            }
        } else {
            return buildSelect( node, builders, statement, cluster, queryInformation );
        }
    }


    protected List<RoutedAlgBuilder> buildSelect( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builders = this.buildDql( node.getInput( i ), builders, statement, cluster, queryInformation );
        }

        if ( node instanceof LogicalDocumentScan ) {
            return Lists.newArrayList( super.handleDocumentScan( (DocumentScan) node, statement, builders.get( 0 ), null ) );
        }

        if ( node instanceof LogicalScan && node.getTable() != null ) {
            AlgOptTableImpl table = (AlgOptTableImpl) node.getTable();

            if ( !(table.getTable() instanceof LogicalTable) ) {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }

            LogicalTable logicalTable = ((LogicalTable) table.getTable());

            if ( logicalTable.getTableId() == -1 ) {
                return handleRelationalOnGraphScan( node, statement, logicalTable, builders, cluster, queryInformation );
            }

            CatalogTable catalogTable = catalog.getTable( logicalTable.getTableId() );

            // Check if table is even horizontal partitioned
            if ( catalogTable.partitionProperty.isPartitioned ) {
                return handleHorizontalPartitioning( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );

            } else {
                // At the moment multiple strategies
                if ( catalogTable.dataPlacements.size() > 1 ) {
                    return handleVerticalPartitioningOrReplication( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );
                }
                return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
            }

        } else if ( node instanceof LogicalValues ) {
            return Lists.newArrayList( super.handleValues( (LogicalValues) node, builders ) );
        } else {
            return Lists.newArrayList( super.handleGeneric( node, builders ) );
        }
    }


    private List<RoutedAlgBuilder> handleRelationalOnGraphScan( AlgNode node, Statement statement, LogicalTable logicalTable, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // todo dl: remove after RowType refactor
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        RexBuilder rexBuilder = algBuilder.getRexBuilder();

        algBuilder.lpgScan( catalog.getSchemas( Catalog.defaultDatabaseId, new Pattern( logicalTable.getLogicalSchemaName() ) ).get( 0 ).id );
        algBuilder.lpgMatch( List.of( algBuilder.lpgNodeMatch( List.of( logicalTable.getLogicalTableName() ) ) ), List.of( "n" ) );
        algBuilder.lpgProject(
                List.of( rexBuilder.makeLpgGetId(), rexBuilder.makeLpgPropertiesExtract(), rexBuilder.makeLpgLabels() ),
                List.of( "id", "properties", "labels" ) );

        AlgNode built = routeGraph( RoutedAlgBuilder.create( statement, cluster ), (AlgNode & LpgAlg) algBuilder.build(), statement );

        builders.get( 0 ).push( new LogicalTransformer(
                node.getCluster(),
                List.of( built ),
                null,
                node.getTraitSet().replace( ModelTrait.RELATIONAL ),
                ModelTrait.GRAPH,
                ModelTrait.RELATIONAL,
                logicalTable.getRowType( algBuilder.getTypeFactory() ), false ) );
        return builders;
    }


    protected List<RoutedAlgBuilder> buildSetOp( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, statement, cluster, queryInformation );

        RoutedAlgBuilder builder0 = RoutedAlgBuilder.create( statement, cluster );
        RoutedAlgBuilder b0 = buildDql( node.getInput( 1 ), Lists.newArrayList( builder0 ), statement, cluster, queryInformation ).get( 0 );

        builders.forEach(
                builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek(), b0.build() ) ) )
        );

        return builders;
    }


    protected List<RoutedAlgBuilder> buildUnion( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, statement, cluster, queryInformation );

        RoutedAlgBuilder builder0 = RoutedAlgBuilder.create( statement, cluster );
        List<RoutedAlgBuilder> b0s = new ArrayList<>();
        for ( AlgNode input : node.getInputs().subList( 1, node.getInputs().size() ) ) {
            b0s.add( buildDql( input, Lists.newArrayList( builder0 ), statement, cluster, queryInformation ).get( 0 ) );
        }

        builders.forEach(
                builder -> builder.replaceTop( node.copy(
                        node.getTraitSet(),
                        ImmutableList.copyOf(
                                Stream.concat(
                                                Stream.of( builder.peek() ),
                                                b0s.stream().map( AlgBuilder::build ) )
                                        .collect( Collectors.toList() ) ) ) )
        );

        return builders;
    }

}
