/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
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


    /**
     * Execute the table scan on the first placement of a table
     *
     * Recursively take the largest placements to build the complete placement (guarantees least amount of placements used => least amount of scans)
     */
    protected static Map<Long, List<AllocationColumn>> selectPlacement( LogicalTable table, List<LogicalColumn> logicalColumns, List<Long> excludedAdapterIds ) {
        // Find the adapter with the most column placements
        long adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Long, List<Long>> entry : Catalog.snapshot().alloc().getColumnPlacementsByAdapter( table.id ).entrySet() ) {
            if ( !excludedAdapterIds.contains( entry.getKey() ) && entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        List<LogicalColumn> missingColumns = new ArrayList<>();

        // Take the adapter with most placements as base and add missing column placements recursively
        List<AllocationColumn> placementList = new ArrayList<>();
        AllocationPlacement longestPlacement = catalog.getSnapshot().alloc().getPlacement( adapterIdWithMostPlacements, table.id ).orElseThrow();
        for ( LogicalColumn column : logicalColumns ) {
            Optional<AllocationColumn> optionalColumn = catalog.getSnapshot().alloc().getColumn( longestPlacement.id, column.id );
            if ( optionalColumn.isPresent() ) {
                placementList.add( optionalColumn.get() );
            } else {
                missingColumns.add( column );
            }
        }

        Map<Long, List<AllocationColumn>> placementToColumns = new HashMap<>();
        placementToColumns.put( longestPlacement.id, placementList );
        if ( !missingColumns.isEmpty() ) {
            List<Long> newExcludedAdapterIds = new ArrayList<>( excludedAdapterIds );
            newExcludedAdapterIds.add( longestPlacement.adapterId );
            Map<Long, List<AllocationColumn>> rest = selectPlacement( table, missingColumns, newExcludedAdapterIds );
            placementToColumns.putAll( rest );
        }
        return placementToColumns;

    }


    public AlgNode recursiveCopy( AlgNode node ) {
        List<AlgNode> inputs = new LinkedList<>();
        if ( node.getInputs() != null && node.getInputs().size() > 0 ) {
            for ( AlgNode input : node.getInputs() ) {
                inputs.add( recursiveCopy( input ) );
            }
        }
        return node.copy( node.getTraitSet(), inputs );
    }


    public RoutedAlgBuilder handleRelScan(
            RoutedAlgBuilder builder,
            Statement statement,
            CatalogEntity entity ) {

        LogicalEntity table;

        if ( entity.unwrap( LogicalTable.class ) != null ) {
            List<AllocationEntity> allocations = statement.getTransaction().getSnapshot().alloc().getFromLogical( entity.id );

            table = entity.unwrap( LogicalEntity.class );
            builder.scan( allocations.get( 0 ) );
        } else if ( entity.unwrap( AllocationTable.class ) != null ) {
            builder.scan( entity.unwrap( AllocationTable.class ) );
            table = statement.getTransaction().getSnapshot().rel().getTable( entity.unwrap( AllocationTable.class ).logicalId ).orElseThrow();
        } else {
            throw new NotImplementedException();
        }

        if ( table.getRowType().getFieldCount() == builder.peek().getRowType().getFieldCount() && !table.getRowType().equals( builder.peek().getRowType() ) ) {
            // we adjust the
            Map<String, Integer> namesIndexMapping = table.getRowType().getFieldList().stream().collect( Collectors.toMap( AlgDataTypeField::getName, AlgDataTypeField::getIndex ) );
            List<Integer> target = builder.peek().getRowType().getFieldList().stream().map( f -> namesIndexMapping.get( f.getName() ) ).collect( Collectors.toList() );
            builder.permute( Mappings.bijection( target ) );
        }

        return builder;
    }


    public DocumentScan<CatalogEntity> handleDocScan(
            DocumentScan<?> scan,
            Statement statement,
            @Nullable List<Long> forbidden ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalNamespace namespace = snapshot.getNamespace( scan.entity.namespaceId );
        if ( namespace.namespaceType == NamespaceType.RELATIONAL ) {
            // cross model queries on relational
            // return handleGraphOnRelational( alg, namespace, statement, allocId );
        } else if ( namespace.namespaceType == NamespaceType.DOCUMENT ) {
            // cross model queries on document
            // return handleGraphOnDocument( alg, namespace, statement, allocId );
        }

        LogicalCollection collection = scan.entity.unwrap( LogicalCollection.class );

        List<Long> placements = snapshot.alloc().getFromLogical( collection.id ).stream().filter( p -> forbidden == null || !forbidden.contains( p.id ) ).map( p -> p.adapterId ).collect( Collectors.toList() );

        for ( long adapterId : placements ) {
            AllocationEntity allocation = snapshot.alloc().getEntity( adapterId, collection.id ).orElseThrow();

            // a native placement was used, we go with that
            return new LogicalDocumentScan( scan.getCluster(), scan.getTraitSet(), allocation );
        }

        throw new RuntimeException( "Error while routing graph query." );

    }


    public RoutedAlgBuilder handleValues( LogicalValues node, RoutedAlgBuilder builder ) {
        return builder.values( node.tuples, node.getRowType() );
    }


    protected List<RoutedAlgBuilder> handleValues( LogicalValues node, List<RoutedAlgBuilder> builders ) {
        return builders.stream().map( builder -> builder.values( node.tuples, node.getRowType() ) ).collect( Collectors.toList() );
    }


    protected RoutedAlgBuilder handleDocuments( LogicalDocumentValues node, RoutedAlgBuilder builder ) {
        return builder.documents( node.documents, node.getRowType() );
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
                throw new RuntimeException( "Unexpected number of input elements: " + node.getInputs().size() );
        }
        return builders;
    }


    public AlgNode buildJoinedScan( Statement statement, AlgOptCluster cluster, LogicalTable table, Map<Long, List<AllocationColumn>> placements ) {
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, cluster );

        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            AlgNode cachedNode = joinedScanCache.getIfPresent( placements.hashCode() );
            if ( cachedNode != null ) {
                return cachedNode;
            }
        }

        for ( Map.Entry<Long, List<AllocationColumn>> placementToColumns : placements.entrySet() ) {

            //List<AllocationPartition> partitions = catalog.getSnapshot().alloc().getPartitionsFromLogical( placements.values().iterator().next().get( 0 ).logicalTableId );
            //long placementId = placementToColumns.getKey();
            List<AllocationColumn> currentPlacements = placementToColumns.getValue();
            long partitionId = placementToColumns.getKey();
            // Sort by adapter
            Map<Long, List<AllocationColumn>> columnsByPlacements = new HashMap<>();
            for ( AllocationColumn column : currentPlacements ) {
                if ( !columnsByPlacements.containsKey( column.placementId ) ) {
                    columnsByPlacements.put( column.placementId, new LinkedList<>() );
                }
                columnsByPlacements.get( column.placementId ).add( column );
            }

            if ( columnsByPlacements.size() == 1 ) {
                long placementId = placements.get( partitionId ).get( 0 ).placementId;
                List<AllocationColumn> columns = columnsByPlacements.get( placementId );

                AllocationEntity cpp = catalog.getSnapshot().alloc().getAlloc( placementId, partitionId ).orElseThrow();

                builder = handleRelScan(
                        builder,
                        statement,
                        cpp );
                // Final project
                buildFinalProject( builder, columns );


            } else if ( columnsByPlacements.size() > 1 ) {
                // We need to join placements on different adapters

                // Get primary key
                long pkid = table.primaryKey;
                List<Long> pkColumnIds = catalog.getSnapshot().rel().getPrimaryKey( pkid ).orElseThrow().columnIds;
                List<LogicalColumn> pkColumns = new ArrayList<>();
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( catalog.getSnapshot().rel().getColumn( pkColumnId ).orElseThrow() );
                }

                // Add primary key
                for ( Entry<Long, List<AllocationColumn>> entry : columnsByPlacements.entrySet() ) {
                    for ( LogicalColumn pkColumn : pkColumns ) {
                        AllocationColumn pkPlacement = catalog.getSnapshot().alloc().getColumn( entry.getValue().get( 0 ).placementId, pkColumn.id ).orElseThrow();
                        if ( !entry.getValue().contains( pkPlacement ) ) {
                            entry.getValue().add( pkPlacement );
                        }
                    }
                }

                Deque<String> queue = new ArrayDeque<>();
                boolean first = true;
                for ( List<AllocationColumn> ccps : columnsByPlacements.values() ) {
                    List<AllocationColumn> ordered = ccps.stream().sorted( Comparator.comparingInt( a -> a.position ) ).collect( Collectors.toList() );
                    AllocationColumn ccp = ccps.get( 0 );
                    AllocationEntity cpp = catalog.getSnapshot().alloc().getAlloc( ccp.placementId, partitionId ).orElseThrow();

                    handleRelScan(
                            builder,
                            statement,
                            cpp );
                    if ( first ) {
                        first = false;
                    } else {
                        List<RexNode> rexNodes = new ArrayList<>();
                        for ( AllocationColumn p : ordered ) {
                            if ( pkColumnIds.contains( p.columnId ) ) {
                                String alias = ccps.get( 0 ).placementId + "_" + p.columnId;
                                rexNodes.add( builder.alias( builder.field( p.getLogicalColumnName() ), alias ) );
                                queue.addFirst( alias );
                                queue.addFirst( p.getLogicalColumnName() );
                            } else {
                                rexNodes.add( builder.field( p.getLogicalColumnName() ) );
                            }
                        }
                        builder.project( rexNodes );
                        List<RexNode> joinConditions = new LinkedList<>();
                        for ( int i = 0; i < pkColumnIds.size(); i++ ) {
                            joinConditions.add( builder.equals(
                                    builder.field( 2, queue.removeFirst() ),
                                    builder.field( 2, queue.removeFirst() ) ) );
                        }
                        builder.join( JoinAlgType.INNER, joinConditions );
                    }
                }
                // Final project
                buildFinalProject( builder, currentPlacements );
            } else {
                throw new GenericRuntimeException( "The table '%s' seems to have no placement. This should not happen!", table.name );
            }
        }

        builder.union( true, placements.size() );

        AlgNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedScanCache.put( placements.hashCode(), node );
        }

        AllocationColumn placement = new ArrayList<>( placements.values() ).get( 0 ).get( 0 );
        // todo dl: remove after RowType refactor
        if ( catalog.getSnapshot().getNamespace( placement.namespaceId ).namespaceType == NamespaceType.DOCUMENT ) {
            AlgDataType rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "d", 0, cluster.getTypeFactory().createPolyType( PolyType.DOCUMENT ) ) ) );
            builder.push( new LogicalTransformer(
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
                .collect( Collectors.toList() );
        for ( LogicalColumn catalogColumn : placementList ) {
            rexNodes.add( builder.field( catalogColumn.name ) );
        }
        builder.project( rexNodes );
    }


    private AlgNode handleOnePartitionScan( Statement statement, AlgOptCluster cluster, Map<Long, List<AllocationColumn>> partitionsColumns, RoutedAlgBuilder builder, Long partitionId ) {
        List<AllocationColumn> columns = partitionsColumns.get( partitionId );

        // each column is one entity
        List<AllocationTable> tables = columns.stream().map( c -> catalog.getSnapshot().alloc().getAlloc( c.placementId, partitionId ).orElseThrow().unwrap( AllocationTable.class ) ).collect( Collectors.toList() );

        List<AlgNode> nodes = tables.stream().map( t -> handleRelScan( RoutedAlgBuilder.create( statement, cluster ), statement, t ).build() ).collect( Collectors.toList() );
        // todo remove multiple scans, add projection
        nodes.forEach( builder::push );

        return nodes.size() == 1 ? builder.build() : builder.union( true ).build();
    }


    public AlgNode handleGraphScan( LogicalLpgScan alg, Statement statement, @Nullable Long allocId, @Nullable List<Long> forbidden ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalNamespace namespace = snapshot.getNamespace( alg.entity.namespaceId );
        if ( namespace.namespaceType == NamespaceType.RELATIONAL ) {
            // cross model queries on relational
            return handleGraphOnRelational( alg, namespace, statement, allocId );
        } else if ( namespace.namespaceType == NamespaceType.DOCUMENT ) {
            // cross model queries on document
            return handleGraphOnDocument( alg, namespace, statement, allocId );
        }

        LogicalGraph catalogGraph = alg.entity.unwrap( LogicalGraph.class );

        List<AlgNode> scans = new ArrayList<>();

        List<Long> placements = snapshot.alloc().getFromLogical( catalogGraph.id ).stream().filter( p -> forbidden == null || !forbidden.contains( p.id ) ).map( p -> p.adapterId ).collect( Collectors.toList() );
        if ( allocId != null ) {
            placements = List.of( allocId );
        }

        for ( long adapterId : placements ) {
            AllocationEntity graph = snapshot.alloc().getEntity( adapterId, catalogGraph.id ).orElseThrow();

            // a native placement was used, we go with that
            return new LogicalLpgScan( alg.getCluster(), alg.getTraitSet(), graph, alg.getRowType() );
        }
        if ( scans.size() < 1 ) {
            throw new RuntimeException( "Error while routing graph query." );
        }

        // rather naive selection strategy
        return scans.get( 0 );
    }


    private AlgNode handleGraphOnRelational( LogicalLpgScan alg, LogicalNamespace namespace, Statement statement, Long placementId ) {
        AlgOptCluster cluster = alg.getCluster();
        List<LogicalTable> tables = Catalog.snapshot().rel().getTables( namespace.id, null );
        List<Pair<String, AlgNode>> scans = tables.stream()
                .map( t -> Pair.of( t.name, buildJoinedScan( statement, cluster, tables.get( 0 ), null ) ) )
                .collect( Collectors.toList() );

        // Builder infoBuilder = cluster.getTypeFactory().builder();
        // infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, GraphType.of(), true );
    }


    private AlgNode handleGraphOnDocument( LogicalLpgScan alg, LogicalNamespace namespace, Statement statement, Long placementId ) {
        AlgOptCluster cluster = alg.getCluster();
        List<LogicalCollection> collections = Catalog.snapshot().doc().getCollections( namespace.id, null );
        List<Pair<String, AlgNode>> scans = collections.stream()
                .map( t -> {
                    RoutedAlgBuilder algBuilder = RoutedAlgBuilder.create( statement, alg.getCluster() );
                    AlgNode scan = algBuilder.documentScan( t ).build();
                    routeDocument( algBuilder, (AlgNode & DocumentAlg) scan, statement );
                    return Pair.of( t.name, algBuilder.build() );
                } )
                .collect( Collectors.toList() );

        // Builder infoBuilder = cluster.getTypeFactory().builder();
        // infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), ModelTrait.DOCUMENT, ModelTrait.GRAPH, GraphType.of(), true );
    }


    @Override
    public List<RoutedAlgBuilder> route( AlgRoot algRoot, Statement statement, LogicalQueryInformation queryInformation ) {
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
