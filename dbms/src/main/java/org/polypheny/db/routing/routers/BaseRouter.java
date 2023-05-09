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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


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
     */
    protected static Map<Long, List<AllocationColumn>> selectPlacement( LogicalTable table ) {
        // Find the adapter with the most column placements
        long adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Long, List<Long>> entry : Catalog.snapshot().alloc().getColumnPlacementsByAdapter( table.id ).entrySet() ) {
            if ( entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        // Take the adapter with most placements as base and add missing column placements
        List<AllocationColumn> placementList = new LinkedList<>();
        for ( LogicalColumn column : Catalog.snapshot().rel().getColumns( table.id ) ) {
            placementList.add( Catalog.snapshot().alloc().getColumnFromLogical( column.id ).get( 0 ) );
        }

        return new HashMap<>() {{
            List<AllocationEntity> allocs = Catalog.snapshot().alloc().getFromLogical( table.id );
            put( allocs.get( 0 ).id, placementList );
        }};
    }


    protected static List<RexNode> addDocumentNodes( AlgDataType rowType, RexBuilder rexBuilder, boolean forceVarchar ) {
        AlgDataType data = rexBuilder.getTypeFactory().createPolyType( PolyType.VARCHAR, 255 );
        return List.of(
                rexBuilder.makeCall(
                        rexBuilder.getTypeFactory().createPolyType( PolyType.VARCHAR, 255 ),
                        OperatorRegistry.get(
                                QueryLanguage.from( "mongo" ),
                                OperatorName.MQL_QUERY_VALUE ),
                        List.of(
                                RexInputRef.of( 0, rowType ),
                                rexBuilder.makeArray(
                                        rexBuilder.getTypeFactory().createArrayType( rexBuilder.getTypeFactory().createPolyType( PolyType.VARCHAR, 255 ), 1 ),
                                        List.of( rexBuilder.makeLiteral( "_id" ) ) ) ) ),
                (forceVarchar
                        ? rexBuilder.makeCall( data,
                        OperatorRegistry.get(
                                QueryLanguage.from( "mongo" ),
                                OperatorName.MQL_JSONIFY ), List.of( RexInputRef.of( 0, rowType ) ) )
                        : RexInputRef.of( 0, rowType ))
        );
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

        if ( entity.unwrap( LogicalTable.class ) != null ) {
            List<AllocationEntity> allocations = statement.getTransaction().getSnapshot().alloc().getFromLogical( entity.id );

            return (RoutedAlgBuilder) builder.scan( allocations.get( 0 ) );
        }
        if ( entity.unwrap( AllocationTable.class ) != null ) {
            return (RoutedAlgBuilder) builder.scan( entity.unwrap( AllocationTable.class ) );
        }
        throw new NotImplementedException();

    }


    protected RoutedAlgBuilder handleDocScan(
            RoutedAlgBuilder builder,
            Statement statement,
            CatalogEntity entity ) {
        List<AllocationEntity> allocations = statement.getTransaction().getSnapshot().alloc().getFromLogical( entity.id );

        if ( entity.unwrap( LogicalCollection.class ) == null ) {
            throw new NotImplementedException();
        }

        return (RoutedAlgBuilder) builder.documentScan( allocations.get( 0 ) );
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


    public AlgNode buildJoinedScan( Statement statement, AlgOptCluster cluster, List<AllocationEntity> allocationEntities ) {
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, cluster );

        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            AlgNode cachedNode = joinedScanCache.getIfPresent( allocationEntities.hashCode() );
            if ( cachedNode != null ) {
                return cachedNode;
            }
        }
        if ( allocationEntities.size() == 1 ) {
            builder = handleRelScan(
                    builder,
                    statement,
                    allocationEntities.get( 0 ) );
            // Final project
            //buildFinalProject( builder, allocationEntities.get( 0 ).unwrap( AllocationTable.class ) );

        }


        /*for ( Map.Entry<Long, List<CatalogColumnPlacement>> partitionToPlacement : allocationEntities.entrySet() ) {
            long partitionId = partitionToPlacement.getKey();
            List<CatalogColumnPlacement> currentPlacements = partitionToPlacement.getValue();
            // Sort by adapter
            /*Map<Long, List<CatalogColumnPlacement>> placementsByAdapter = new HashMap<>();
            for ( CatalogColumnPlacement placement : currentPlacements ) {
                if ( !placementsByAdapter.containsKey( placement.adapterId ) ) {
                    placementsByAdapter.put( placement.adapterId, new LinkedList<>() );
                }
                placementsByAdapter.get( placement.adapterId ).add( placement );
            }

            if ( placementsByAdapter.size() == 1 ) {
                // List<CatalogColumnPlacement> ccps = placementsByAdapter.values().iterator().next();
                // CatalogColumnPlacement ccp = ccps.get( 0 );
                // CatalogPartitionPlacement cpp = catalog.getPartitionPlacement( ccp.adapterId, partitionId );
                partitionId = snapshot.alloc().getAllocation( partitionId, currentPlacements.get( 0 ).tableId ).id;

                builder = handleRelScan(
                        builder,
                        statement,
                        partitionId );
                // Final project
                buildFinalProject( builder, currentPlacements );

            } else if ( placementsByAdapter.size() > 1 ) {
                // We need to join placements on different adapters

                // Get primary key
                LogicalRelSnapshot relSnapshot = snapshot.rel().namespaceId );
                long pkid = relSnapshot.getTable( currentPlacements.get( 0 ).tableId ).primaryKey;
                List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).columnIds;
                List<LogicalColumn> pkColumns = new LinkedList<>();
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( relSnapshot.getColumn( pkColumnId ) );
                }

                // Add primary key
                for ( Entry<Long, List<CatalogColumnPlacement>> entry : placementsByAdapter.entrySet() ) {
                    for ( LogicalColumn pkColumn : pkColumns ) {
                        CatalogColumnPlacement pkPlacement = Catalog.getInstance().getSnapshot().alloc().getColumnFromLogical( pkColumn.id ).get( 0 );
                        if ( !entry.getValue().contains( pkPlacement ) ) {
                            entry.getValue().add( pkPlacement );
                        }
                    }
                }

                Deque<String> queue = new LinkedList<>();
                boolean first = true;
                for ( List<CatalogColumnPlacement> ccps : placementsByAdapter.values() ) {
                    CatalogColumnPlacement ccp = ccps.get( 0 );
                    CatalogPartitionPlacement cpp = Catalog.getInstance().getSnapshot().alloc().getPartitionPlacement( ccp.adapterId, partitionId );

                    handleRelScan(
                            builder,
                            statement,
                            cpp.partitionId
                    );
                    if ( first ) {
                        first = false;
                    } else {
                        ArrayList<RexNode> rexNodes = new ArrayList<>();
                        for ( CatalogColumnPlacement p : ccps ) {
                            if ( pkColumnIds.contains( p.columnId ) ) {
                                String alias = ccps.get( 0 ).adapterId + "_" + p.getLogicalColumnName();
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
                            joinConditions.add( builder.call(
                                    OperatorRegistry.get( OperatorName.EQUALS ),
                                    builder.field( 2, ccp.getLogicalTableName() + "_" + partitionId, queue.removeFirst() ),
                                    builder.field( 2, ccp.getLogicalTableName() + "_" + partitionId, queue.removeFirst() ) ) );
                        }
                        builder.join( JoinAlgType.INNER, joinConditions );
                    }
                }
                // Final project
                buildFinalProject( builder, currentPlacements );
            } else {
                throw new RuntimeException( "The table '" + currentPlacements.get( 0 ).getLogicalTableName() + "' seems to have no placement. This should not happen!" );
            }
        }*/

        builder.union( true, allocationEntities.size() );

        AlgNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedScanCache.put( allocationEntities.hashCode(), node );
        }

        AllocationColumn placement = catalog.getSnapshot().alloc().getColumns( allocationEntities.get( 0 ).id ).get( 0 );
        // todo dl: remove after RowType refactor
        if ( Catalog.snapshot().getNamespace( placement.namespaceId ).namespaceType == NamespaceType.DOCUMENT ) {
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


    public AlgNode handleGraphScan( LogicalLpgScan alg, Statement statement, @Nullable Long placementId ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalNamespace namespace = snapshot.getNamespace( alg.entity.id );
        if ( namespace.namespaceType == NamespaceType.RELATIONAL ) {
            // cross model queries on relational
            return handleGraphOnRelational( alg, namespace, statement, placementId );
        } else if ( namespace.namespaceType == NamespaceType.DOCUMENT ) {
            // cross model queries on document
            return handleGraphOnDocument( alg, namespace, statement, placementId );
        }

        LogicalGraph catalogGraph = alg.entity.unwrap( LogicalGraph.class );

        List<AlgNode> scans = new ArrayList<>();

        List<Long> placements = snapshot.alloc().getFromLogical( catalogGraph.id ).stream().map( p -> p.adapterId ).collect( Collectors.toList() );
        if ( placementId != null ) {
            placements = List.of( placementId );
        }

        for ( long adapterId : placements ) {
            AllocationEntity graph = snapshot.alloc().getAllocation( catalogGraph.id, adapterId );

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
                .map( t -> Pair.of( t.name, buildJoinedScan( statement, cluster, null ) ) )
                .collect( Collectors.toList() );

        Builder infoBuilder = cluster.getTypeFactory().builder();
        infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, infoBuilder.build(), true );
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

        Builder infoBuilder = cluster.getTypeFactory().builder();
        infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), ModelTrait.DOCUMENT, ModelTrait.GRAPH, infoBuilder.build(), true );
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
            builder.push( handleDocScan( builder, statement, alg.getEntity() ).build() );
            return alg;
        } else if ( alg instanceof DocumentValues ) {
            return alg;
        }
        throw new UnsupportedOperationException();
    }


}
