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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalGraph.SubstitutionGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.ColumnDistribution;
import org.polypheny.db.routing.ColumnDistribution.FullPartition;
import org.polypheny.db.routing.ColumnDistribution.PartialPartition;
import org.polypheny.db.routing.ColumnDistribution.RoutedDistribution;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Base Router for all routers including DML, DQL and Cached plans.
 */
@Slf4j
public abstract class BaseRouter implements Router {

    public static final Cache<Integer, AlgNode> joinedScanCache = CacheBuilder.newBuilder()
            .maximumSize( RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.getInteger() )
            .build();

    final static Catalog catalog = Catalog.getInstance();


    static {
        RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.setRequiresRestart( true );
    }


    public AlgNode recursiveCopy( AlgNode node ) {
        List<AlgNode> inputs = new LinkedList<>();
        if ( node.getInputs() != null && !node.getInputs().isEmpty() ) {
            for ( AlgNode input : node.getInputs() ) {
                inputs.add( recursiveCopy( input ) );
            }
        }
        return node.copy( node.getTraitSet(), inputs );
    }


    public RoutedAlgBuilder handleRelScan(
            RoutedAlgBuilder builder,
            Statement statement,
            Entity entity ) {

        LogicalEntity table;

        if ( entity.unwrap( LogicalTable.class ).isPresent() ) {
            List<AllocationEntity> allocations = statement.getTransaction().getSnapshot().alloc().getFromLogical( entity.id );
            table = entity.unwrap( LogicalTable.class ).orElseThrow();
            builder.relScan( allocations.get( 0 ) );
        } else if ( entity.unwrap( AllocationTable.class ).isPresent() ) {
            builder.relScan( entity.unwrap( AllocationTable.class ).get() );
            table = statement.getTransaction().getSnapshot().rel().getTable( entity.unwrap( AllocationTable.class ).orElseThrow().logicalId ).orElseThrow();
        } else {
            throw new NotImplementedException();
        }

        if ( table.getTupleType().getFieldCount() == builder.peek().getTupleType().getFieldCount() && !table.getTupleType().equals( builder.peek().getTupleType() ) ) {
            // we adjust the
            Map<String, Integer> namesIndexMapping = table.getTupleType().getFields().stream().collect( Collectors.toMap( AlgDataTypeField::getName, AlgDataTypeField::getIndex ) );
            List<Integer> target = builder.peek().getTupleType().getFields().stream().map( f -> namesIndexMapping.get( f.getName() ) ).toList();
            builder.permute( Mappings.bijection( target ) );
        }

        return builder;
    }


    public DocumentScan<Entity> handleDocScan(
            DocumentScan<?> scan,
            Statement statement,
            @Nullable List<Long> excludedPlacements ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        List<AllocationPlacement> placements = snapshot.alloc().getPlacementsFromLogical( scan.entity.id ).stream().filter( p -> excludedPlacements == null || !excludedPlacements.contains( p.id ) ).toList();

        List<AllocationPartition> partitions = snapshot.alloc().getPartitionsFromLogical( scan.entity.id );

        for ( AllocationPlacement placement : placements ) {
            AllocationEntity allocation = snapshot.alloc().getAlloc( placement.id, partitions.get( 0 ).id ).orElseThrow();

            // a native placement was used, we go with that
            return new LogicalDocumentScan( scan.getCluster(), scan.getTraitSet(), allocation.withName( scan.entity.name ) );
        }

        throw new GenericRuntimeException( "Error while routing graph query." );

    }


    public RoutedAlgBuilder handleValues( LogicalRelValues node, RoutedAlgBuilder builder ) {
        return builder.values( node.tuples, node.getTupleType() );
    }


    protected List<RoutedAlgBuilder> handleValues( LogicalRelValues node, List<RoutedAlgBuilder> builders ) {
        return builders.stream().map( builder -> builder.values( node.tuples, node.getTupleType() ) ).toList();
    }


    protected RoutedAlgBuilder handleDocuments( LogicalDocumentValues node, RoutedAlgBuilder builder ) {
        return builder.documents( node.documents, node.getTupleType() );
    }


    public RoutedAlgBuilder handleGeneric( AlgNode node, RoutedAlgBuilder builder ) {
        final List<RoutedAlgBuilder> result = handleGeneric( node, Lists.newArrayList( builder ) );
        if ( result.size() > 1 ) {
            log.error( "Single handle generic with multiple results " );
        }
        return result.get( 0 );
    }


    protected List<RoutedAlgBuilder> handleGeneric( AlgNode node, List<RoutedAlgBuilder> builders ) {
        switch ( node.getInputs().size() ) {
            case 1:
                builders.forEach(
                        builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) )
                );
                break;
            case 2:
                builders.forEach(
                        builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 1 ), builder.peek( 0 ) ) ), 2 )
                );
                break;
            default:
                throw new GenericRuntimeException( "Unexpected number of input elements: " + node.getInputs().size() );
        }
        return builders;
    }


    public AlgNode buildJoinedScan( ColumnDistribution columnDistribution, RoutingContext context ) {
        RoutedAlgBuilder builder = context.getRoutedAlgBuilder();

        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            AlgNode cachedNode = joinedScanCache.getIfPresent( columnDistribution.hashCode() );
            if ( cachedNode != null ) {
                return cachedNode;
            }
        }
        RoutedDistribution routedDistribution = columnDistribution.route();
        context.routedDistribution = routedDistribution;

        for ( FullPartition partition : routedDistribution.partitions() ) {

            if ( !partition.needsJoin() ) {
                PartialPartition partitionColumns = partition.partials().get( 0 );

                builder = handleRelScan(
                        builder,
                        context.getStatement(),
                        partitionColumns.entity() );
                // Final project
                buildFinalProject( builder, partitionColumns.columns() );

            } else {
                // We need to join placements on different adapters

                Deque<String> queue = new ArrayDeque<>();
                boolean first = true;
                for ( PartialPartition allocation : partition.partials() ) {
                    List<AllocationColumn> ordered = allocation.columns().stream().sorted( Comparator.comparingInt( a -> a.position ) ).toList();
                    handleRelScan( builder, context.getStatement(), allocation.entity() );

                    RoutedAlgBuilder finalBuilder = builder;
                    // project away all unnecessary columns
                    builder.project( ordered.stream().map( a -> finalBuilder.field( a.getLogicalColumnName() ) ).toList() );
                    if ( first ) {
                        first = false;
                        continue;
                    }

                    List<RexNode> rexNodes = new ArrayList<>();
                    for ( AllocationColumn p : ordered ) {
                        if ( columnDistribution.getPrimaryIds().contains( p.columnId ) ) {
                            String alias = p.placementId + "_" + p.columnId;
                            rexNodes.add( builder.alias( builder.field( p.getLogicalColumnName() ), alias ) );
                            queue.addFirst( alias );
                            queue.addFirst( p.getLogicalColumnName() );
                        } else {
                            rexNodes.add( builder.field( p.getLogicalColumnName() ) );
                        }
                    }
                    builder.project( rexNodes );
                    List<RexNode> joinConditions = new ArrayList<>();
                    for ( int i = 0; i < columnDistribution.getPrimaryIds().size(); i++ ) {
                        joinConditions.add( builder.equals(
                                builder.field( 2, queue.removeFirst() ),
                                builder.field( 2, queue.removeFirst() ) ) );
                    }
                    builder.join( JoinAlgType.INNER, joinConditions );

                }
                // Final project
                buildFinalProject( builder, partition.getOrderedColumns() );
            }
        }

        if ( columnDistribution.getPartitions().size() == 1 ) {
            return builder.build();
        }

        builder.union( true, columnDistribution.getPartitions().size() );

        AlgNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedScanCache.put( columnDistribution.hashCode(), node );
        }

        // todo dl: remove after RowType refactor
        if ( catalog.getSnapshot().getNamespace( columnDistribution.getTable().namespaceId ).orElseThrow().dataModel == DataModel.DOCUMENT ) {
            AlgDataType rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( 1L, "d", 0, context.getCluster().getTypeFactory().createPolyType( PolyType.DOCUMENT ) ) ) );
            builder.push( LogicalTransformer.create(
                    node.getCluster(),
                    List.of( node ),
                    null,
                    ModelTrait.DOCUMENT,
                    ModelTrait.RELATIONAL,
                    rowType,
                    true ) );
            node = builder.build();
        }

        return node;
    }


    private void buildFinalProject( RoutedAlgBuilder builder, List<AllocationColumn> currentPlacements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        List<LogicalColumn> placementList = currentPlacements.stream()
                .map( col -> catalog.getSnapshot().rel().getColumn( col.columnId ).orElseThrow() )
                .sorted( Comparator.comparingInt( col -> col.position ) )
                .toList();
        for ( LogicalColumn catalogColumn : placementList ) {
            rexNodes.add( builder.field( catalogColumn.name ) );
        }
        builder.project( rexNodes );
    }


    public AlgNode handleGraphScan( LogicalLpgScan alg, Statement statement, @Nullable AllocationEntity targetAlloc, @Nullable List<Long> excludedPlacements ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalNamespace namespace = snapshot.getNamespace( alg.entity.namespaceId ).orElseThrow();

        LogicalGraph graph = alg.entity.unwrap( LogicalGraph.class ).orElseThrow();

        List<AllocationPlacement> placements = snapshot.alloc().getPlacementsFromLogical( graph.id ).stream().filter( p -> excludedPlacements == null || !excludedPlacements.contains( p.id ) ).toList();
        if ( targetAlloc != null ) {
            return new LogicalLpgScan( alg.getCluster(), alg.getTraitSet(), targetAlloc, alg.getTupleType() );
        }

        for ( AllocationPlacement placement : placements ) {
            List<AllocationPartition> partitions = snapshot.alloc().getPartitionsFromLogical( graph.id );
            if ( partitions.size() != 1 ) {
                throw new GenericRuntimeException( "Graphs with multiple partitions are not supported yet." );
            }
            Optional<AllocationEntity> optEntity = snapshot.alloc().getAlloc( placement.id, partitions.get( 0 ).id );
            if ( optEntity.isEmpty() ) {
                throw new GenericRuntimeException( "Error while routing graph query." );
            }
            AllocationEntity entity = optEntity.orElseThrow();

            // a native placement was used, we go with that
            return new LogicalLpgScan( alg.getCluster(), alg.getTraitSet(), entity, alg.getTupleType() );
        }

        // cross-modal?
        if ( namespace.dataModel == DataModel.DOCUMENT || namespace.dataModel == DataModel.RELATIONAL ) {
            return handleGraphCrossModel( alg, statement, graph, namespace, snapshot );
        }

        throw new GenericRuntimeException( "Error while routing graph query." );

        // rather naive selection strategy
    }


    @NotNull
    private LogicalTransformer handleGraphCrossModel( LogicalLpgScan alg, Statement statement, LogicalGraph graph, LogicalNamespace namespace, Snapshot snapshot ) {
        if ( graph.unwrap( SubstitutionGraph.class ).isEmpty() ) {
            throw new GenericRuntimeException( "Error while routing cross-model graph query." );
        }
        SubstitutionGraph substitutionGraph = graph.unwrap( SubstitutionGraph.class ).get();
        List<Pair<String, AlgNode>> scans = new ArrayList<>();
        List<PolyString> names = substitutionGraph.names;
        if ( names.isEmpty() ) {
            // no label means all entites
            names = namespace.dataModel == DataModel.DOCUMENT ? snapshot.doc().getCollections( graph.namespaceId, null ).stream().map( c -> new PolyString( c.name ) ).toList() : snapshot.rel().getTables( graph.namespaceId, null ).stream().map( t -> new PolyString( t.name ) ).toList();
        }

        for ( PolyString name : names ) {
            if ( namespace.dataModel == DataModel.DOCUMENT ) {
                snapshot.doc().getCollection( graph.id, name.value ).ifPresent( c -> {
                    RoutedAlgBuilder algBuilder = RoutedAlgBuilder.create( statement, alg.getCluster() );
                    AlgNode scan = algBuilder.documentScan( c ).build();
                    routeDocument( algBuilder, scan, statement );
                    scans.add( Pair.of( name.value, algBuilder.build() ) );
                } );
            } else if ( namespace.dataModel == DataModel.RELATIONAL ) {
                snapshot.rel().getTable( graph.namespaceId, name.value )
                        .ifPresent( t ->
                                scans.add( Pair.of( name.value, handleRelScan( RoutedAlgBuilder.create( statement, alg.getCluster() ), statement, t ).build() ) ) );
            }
        }

        return new LogicalTransformer( alg.getCluster(), alg.getTraitSet(), Pair.right( scans ), Pair.left( scans ), namespace.dataModel.getModelTrait(), ModelTrait.GRAPH, GraphType.of(), true );
    }


    @Override
    public List<RoutedAlgBuilder> route( AlgRoot algRoot, RoutingContext context ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void resetCaches() {
        throw new UnsupportedOperationException();
    }


    @Override
    public <T extends AlgNode & LpgAlg> AlgNode routeGraph( RoutedAlgBuilder builder, T alg, Statement statement ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public AlgNode routeDocument( RoutedAlgBuilder builder, AlgNode alg, Statement statement ) {
        if ( alg.getInputs().size() == 1 ) {
            routeDocument( builder, alg.getInput( 0 ), statement );
            if ( builder.stackSize() > 0 ) {
                alg.replaceInput( 0, builder.build() );
            }
            return alg;
        } else if ( alg instanceof DocumentScan ) {
            builder.push( handleDocScan( (DocumentScan<?>) alg, statement, null ) );
            return alg;
        } else if ( alg instanceof DocumentValues ) {
            return alg;
        }
        throw new UnsupportedOperationException();
    }


}
