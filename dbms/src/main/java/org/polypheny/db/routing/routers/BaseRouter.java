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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.TranslatableGraph;
import org.polypheny.db.schema.graph.Graph;
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
    protected static Map<Long, List<CatalogColumnPlacement>> selectPlacement( CatalogTable table ) {
        // Find the adapter with the most column placements
        int adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Integer, ImmutableList<Long>> entry : catalog.getColumnPlacementsByAdapter( table.id ).entrySet() ) {
            if ( entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        // Take the adapter with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( long cid : table.fieldIds ) {
            if ( catalog.getDataPlacement( adapterIdWithMostPlacements, table.id ).columnPlacementsOnAdapter.contains( cid ) ) {
                placementList.add( Catalog.getInstance().getColumnPlacement( adapterIdWithMostPlacements, cid ) );
            } else {
                placementList.add( Catalog.getInstance().getColumnPlacement( cid ).get( 0 ) );
            }
        }

        return new HashMap<Long, List<CatalogColumnPlacement>>() {{
            put( table.partitionProperty.partitionIds.get( 0 ), placementList );
        }};
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


    public RoutedAlgBuilder handleScan(
            RoutedAlgBuilder builder,
            Statement statement,
            long tableId,
            String storeUniqueName,
            String logicalSchemaName,
            String logicalTableName,
            String physicalSchemaName,
            String physicalTableName,
            long partitionId ) {

        AlgNode node = builder.scan( ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName( storeUniqueName, logicalSchemaName, physicalSchemaName ),
                logicalTableName + "_" + partitionId ) ).build();

        builder.push( node );

        return builder;
    }


    public RoutedAlgBuilder handleValues( LogicalValues node, RoutedAlgBuilder builder ) {
        return builder.values( node.tuples, node.getRowType() );
    }


    protected List<RoutedAlgBuilder> handleValues( LogicalValues node, List<RoutedAlgBuilder> builders ) {
        return builders.stream().map( builder -> builder.values( node.tuples, node.getRowType() ) ).collect( Collectors.toList() );
    }


    protected RoutedAlgBuilder handleDocuments( LogicalDocumentValues node, RoutedAlgBuilder builder ) {
        return builder.documents( node.getDocumentTuples(), node.getRowType() );
    }


    public RoutedAlgBuilder handleGeneric( AlgNode node, RoutedAlgBuilder builder ) {
        final List<RoutedAlgBuilder> result = handleGeneric( node, Lists.newArrayList( builder ) );
        if ( result.size() > 1 ) {
            log.error( "Single handle generic with multiple results " );
        }
        return result.get( 0 );
    }


    protected List<RoutedAlgBuilder> handleGeneric( AlgNode node, List<RoutedAlgBuilder> builders ) {
        if ( node.getInputs().size() == 1 ) {
            builders.forEach(
                    builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) )
            );
        } else if ( node.getInputs().size() == 2 ) { // Joins, SetOperations
            builders.forEach(
                    builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 1 ), builder.peek( 0 ) ) ), 2 )
            );
        } else {
            throw new RuntimeException( "Unexpected number of input elements: " + node.getInputs().size() );
        }
        return builders;
    }


    public AlgNode buildJoinedScan( Statement statement, AlgOptCluster cluster, Map<Long, List<CatalogColumnPlacement>> placements ) {
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, cluster );

        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            AlgNode cachedNode = joinedScanCache.getIfPresent( placements.hashCode() );
            if ( cachedNode != null ) {
                return cachedNode;
            }
        }

        for ( Map.Entry<Long, List<CatalogColumnPlacement>> partitionToPlacement : placements.entrySet() ) {
            long partitionId = partitionToPlacement.getKey();
            List<CatalogColumnPlacement> currentPlacements = partitionToPlacement.getValue();
            // Sort by adapter
            Map<Integer, List<CatalogColumnPlacement>> placementsByAdapter = new HashMap<>();
            for ( CatalogColumnPlacement placement : currentPlacements ) {
                if ( !placementsByAdapter.containsKey( placement.adapterId ) ) {
                    placementsByAdapter.put( placement.adapterId, new LinkedList<>() );
                }
                placementsByAdapter.get( placement.adapterId ).add( placement );
            }

            if ( placementsByAdapter.size() == 1 ) {
                List<CatalogColumnPlacement> ccps = placementsByAdapter.values().iterator().next();
                CatalogColumnPlacement ccp = ccps.get( 0 );
                CatalogPartitionPlacement cpp = catalog.getPartitionPlacement( ccp.adapterId, partitionId );

                builder = handleScan(
                        builder,
                        statement,
                        ccp.tableId,
                        ccp.adapterUniqueName,
                        ccp.getLogicalSchemaName(),
                        ccp.getLogicalTableName(),
                        ccp.physicalSchemaName,
                        cpp.physicalTableName,
                        cpp.partitionId );
                // Final project
                buildFinalProject( builder, currentPlacements );

            } else if ( placementsByAdapter.size() > 1 ) {
                // We need to join placements on different adapters

                // Get primary key
                long pkid = catalog.getTable( currentPlacements.get( 0 ).tableId ).primaryKey;
                List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
                List<CatalogColumn> pkColumns = new LinkedList<>();
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( catalog.getColumn( pkColumnId ) );
                }

                // Add primary key
                for ( Entry<Integer, List<CatalogColumnPlacement>> entry : placementsByAdapter.entrySet() ) {
                    for ( CatalogColumn pkColumn : pkColumns ) {
                        CatalogColumnPlacement pkPlacement = catalog.getColumnPlacement( entry.getKey(), pkColumn.id );
                        if ( !entry.getValue().contains( pkPlacement ) ) {
                            entry.getValue().add( pkPlacement );
                        }
                    }
                }

                Deque<String> queue = new LinkedList<>();
                boolean first = true;
                for ( List<CatalogColumnPlacement> ccps : placementsByAdapter.values() ) {
                    CatalogColumnPlacement ccp = ccps.get( 0 );
                    CatalogPartitionPlacement cpp = catalog.getPartitionPlacement( ccp.adapterId, partitionId );

                    handleScan(
                            builder,
                            statement,
                            ccp.tableId,
                            ccp.adapterUniqueName,
                            ccp.getLogicalSchemaName(),
                            ccp.getLogicalTableName(),
                            ccp.physicalSchemaName,
                            cpp.physicalTableName,
                            cpp.partitionId );
                    if ( first ) {
                        first = false;
                    } else {
                        ArrayList<RexNode> rexNodes = new ArrayList<>();
                        for ( CatalogColumnPlacement p : ccps ) {
                            if ( pkColumnIds.contains( p.columnId ) ) {
                                String alias = ccps.get( 0 ).adapterUniqueName + "_" + p.getLogicalColumnName();
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
        }

        builder.union( true, placements.size() );

        AlgNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedScanCache.put( placements.hashCode(), node );
        }

        CatalogColumnPlacement placement = new ArrayList<>( placements.values() ).get( 0 ).get( 0 );
        // todo dl: remove after RowType refactor
        if ( statement.getTransaction().getCatalogReader().getTable( List.of( placement.getLogicalSchemaName(), placement.getLogicalTableName() ) ).getTable().getSchemaType() == NamespaceType.DOCUMENT ) {
            AlgDataType rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "d", 0, cluster.getTypeFactory().createPolyType( PolyType.DOCUMENT ) ) ) );
            builder.push( new LogicalTransformer(
                    node.getCluster(),
                    List.of( node ),
                    null,
                    node.getTraitSet().replace( ModelTrait.RELATIONAL ),
                    ModelTrait.DOCUMENT,
                    ModelTrait.RELATIONAL,
                    rowType,
                    true ) );
            node = builder.build();
        }

        return node;
    }


    private void buildFinalProject( RoutedAlgBuilder builder, List<CatalogColumnPlacement> currentPlacements ) {
        List<RexNode> rexNodes = new ArrayList<>();
        List<CatalogColumn> placementList = currentPlacements.stream()
                .map( col -> catalog.getColumn( col.columnId ) )
                .sorted( Comparator.comparingInt( col -> col.position ) )
                .collect( Collectors.toList() );
        for ( CatalogColumn catalogColumn : placementList ) {
            rexNodes.add( builder.field( catalogColumn.name ) );
        }
        builder.project( rexNodes );
    }


    public AlgNode handleGraphScan( LogicalLpgScan alg, Statement statement, @Nullable Integer placementId ) {
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();

        Catalog catalog = Catalog.getInstance();

        CatalogSchema namespace = catalog.getSchema( alg.getGraph().getId() );
        if ( namespace.namespaceType == NamespaceType.RELATIONAL ) {
            // cross model queries on relational
            return handleGraphOnRelational( alg, namespace, statement, placementId );
        } else if ( namespace.namespaceType == NamespaceType.DOCUMENT ) {
            // cross model queries on document
            return handleGraphOnDocument( alg, namespace, statement, placementId );
        }

        CatalogGraphDatabase catalogGraph = catalog.getGraph( alg.getGraph().getId() );

        List<AlgNode> scans = new ArrayList<>();

        List<Integer> placements = catalogGraph.placements;
        if ( placementId != null ) {
            placements = List.of( placementId );
        }

        for ( int adapterId : placements ) {
            CatalogAdapter adapter = catalog.getAdapter( adapterId );
            CatalogGraphPlacement graphPlacement = catalog.getGraphPlacement( catalogGraph.id, adapterId );
            String name = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, catalogGraph.name, graphPlacement.physicalName );

            Graph graph = reader.getGraph( name );

            if ( !(graph instanceof TranslatableGraph) ) {
                // needs substitution later on
                scans.add( getRelationalScan( alg, adapterId, statement ) );
                continue;
            }

            // a native placement was used, we go with that
            return new LogicalLpgScan( alg.getCluster(), alg.getTraitSet(), (TranslatableGraph) graph, alg.getRowType() );
        }
        if ( scans.size() < 1 ) {
            throw new RuntimeException( "Error while routing graph query." );
        }

        // rather naive selection strategy
        return scans.get( 0 );
    }


    private AlgNode handleGraphOnRelational( LogicalLpgScan alg, CatalogSchema namespace, Statement statement, Integer placementId ) {
        AlgOptCluster cluster = alg.getCluster();
        List<CatalogTable> tables = catalog.getTables( Catalog.defaultDatabaseId, new Pattern( namespace.name ), null );
        List<Pair<String, AlgNode>> scans = tables.stream()
                .map( t -> Pair.of( t.name, buildJoinedScan( statement, cluster, selectPlacement( t ) ) ) )
                .collect( Collectors.toList() );

        Builder infoBuilder = cluster.getTypeFactory().builder();
        infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), alg.getTraitSet().replace( ModelTrait.GRAPH ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, infoBuilder.build(), true );
    }


    private AlgNode handleGraphOnDocument( LogicalLpgScan alg, CatalogSchema namespace, Statement statement, Integer placementId ) {
        AlgOptCluster cluster = alg.getCluster();
        List<CatalogCollection> collections = catalog.getCollections( namespace.id, null );
        List<Pair<String, AlgNode>> scans = collections.stream()
                .map( t -> {
                    RoutedAlgBuilder algBuilder = RoutedAlgBuilder.create( statement, alg.getCluster() );
                    AlgOptTable collection = statement.getTransaction().getCatalogReader().getCollection( List.of( t.getNamespaceName(), t.name ) );
                    AlgNode scan = algBuilder.documentScan( collection ).build();
                    routeDocument( algBuilder, (AlgNode & DocumentAlg) scan, statement );
                    return Pair.of( t.name, algBuilder.build() );
                } )
                .collect( Collectors.toList() );

        Builder infoBuilder = cluster.getTypeFactory().builder();
        infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), alg.getTraitSet().replace( ModelTrait.GRAPH ), ModelTrait.DOCUMENT, ModelTrait.GRAPH, infoBuilder.build(), true );
    }


    public AlgNode getRelationalScan( LogicalLpgScan alg, int adapterId, Statement statement ) {
        CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( alg.getGraph().getId() );

        PreparingTable nodesTable = getSubstitutionTable( statement, mapping.nodesId, mapping.idNodeId, adapterId );
        PreparingTable nodePropertiesTable = getSubstitutionTable( statement, mapping.nodesPropertyId, mapping.idNodesPropertyId, adapterId );
        PreparingTable edgesTable = getSubstitutionTable( statement, mapping.edgesId, mapping.idEdgeId, adapterId );

        PreparingTable edgePropertiesTable = getSubstitutionTable( statement, mapping.edgesPropertyId, mapping.idEdgesPropertyId, adapterId );

        AlgNode node = buildSubstitutionJoin( alg, nodesTable, nodePropertiesTable );

        AlgNode edge = buildSubstitutionJoin( alg, edgesTable, edgePropertiesTable );

        return LogicalTransformer.create( List.of( node, edge ), alg.getTraitSet().replace( ModelTrait.RELATIONAL ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, alg.getRowType() );

    }


    protected PreparingTable getSubstitutionTable( Statement statement, long tableId, long columnId, int adapterId ) {
        CatalogTable nodes = Catalog.getInstance().getTable( tableId );
        CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( adapterId, columnId );
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.adapterUniqueName,
                        nodes.getNamespaceName(),
                        placement.physicalSchemaName
                ),
                nodes.name + "_" + nodes.partitionProperty.partitionIds.get( 0 ) );

        return statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
    }


    protected AlgNode buildSubstitutionJoin( AlgNode alg, PreparingTable nodesTable, PreparingTable nodePropertiesTable ) {
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.RELATIONAL );
        LogicalScan nodes = new LogicalScan( alg.getCluster(), out, nodesTable );
        LogicalScan nodesProperty = new LogicalScan( alg.getCluster(), out, nodePropertiesTable );

        RexBuilder builder = alg.getCluster().getRexBuilder();

        RexNode nodeCondition = builder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                builder.makeInputRef( nodes.getRowType().getFieldList().get( 0 ).getType(), 0 ),
                builder.makeInputRef( nodesProperty.getRowType().getFieldList().get( 0 ).getType(), nodes.getRowType().getFieldList().size() ) );

        return new LogicalJoin( alg.getCluster(), out, nodes, nodesProperty, nodeCondition, Set.of(), JoinAlgType.LEFT, false, ImmutableList.of() );
    }


    protected RoutedAlgBuilder handleDocumentScan( DocumentScan alg, Statement statement, RoutedAlgBuilder builder, Integer adapterId ) {
        Catalog catalog = Catalog.getInstance();
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();

        if ( alg.getCollection().getTable().getSchemaType() != NamespaceType.DOCUMENT ) {
            if ( alg.getCollection().getTable().getSchemaType() == NamespaceType.GRAPH ) {
                return handleDocumentOnGraph( alg, statement, builder );
            }

            return handleTransformerDocScan( alg, statement, builder );
        }

        CatalogCollection collection = catalog.getCollection( alg.getCollection().getTable().getTableId() );

        List<RoutedAlgBuilder> scans = new ArrayList<>();

        List<Integer> placements = collection.placements;
        if ( adapterId != null ) {
            placements = List.of( adapterId );
        }

        for ( Integer placementId : placements ) {
            CatalogAdapter adapter = catalog.getAdapter( placementId );
            NamespaceType sourceModel = alg.getCollection().getTable().getSchemaType();

            if ( !adapter.getSupportedNamespaces().contains( sourceModel ) ) {
                // document on relational
                scans.add( handleDocumentOnRelational( alg, placementId, statement, builder ) );
                continue;
            }
            CatalogCollectionPlacement placement = catalog.getCollectionPlacement( collection.id, placementId );
            String namespaceName = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, collection.getNamespaceName(), placement.physicalNamespaceName );
            String collectionName = collection.name + "_" + placement.id;
            AlgOptTable collectionTable = reader.getCollection( List.of( namespaceName, collectionName ) );
            // we might previously have pushed the non-native transformer
            builder.clear();
            return builder.push( LogicalDocumentScan.create( alg.getCluster(), collectionTable ) );
        }

        if ( scans.size() < 1 ) {
            throw new RuntimeException( "No placement found for the document." );
        }

        // rather basic selection for non-native
        return scans.get( 0 );
    }


    private RoutedAlgBuilder handleTransformerDocScan( DocumentScan alg, Statement statement, RoutedAlgBuilder builder ) {
        AlgNode scan = buildJoinedScan( statement, alg.getCluster(), selectPlacement( catalog.getTable( alg.getCollection().getTable().getTableId() ) ) );

        //List<RoutedAlgBuilder> scans = ((AbstractDqlRouter) RoutingManager.getInstance().getRouters().get( 0 )).buildDql( scan, List.of( builder ), statement, alg.getCluster(), queryInformation );
        builder.push( scan );
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.RELATIONAL );
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), null, out.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, alg.getRowType(), false ) );
        return builder;
    }


    @NotNull
    private RoutedAlgBuilder handleDocumentOnRelational( DocumentScan node, Integer adapterId, Statement statement, RoutedAlgBuilder builder ) {
        List<CatalogColumn> columns = catalog.getColumns( node.getCollection().getTable().getTableId() );
        AlgTraitSet out = node.getTraitSet().replace( ModelTrait.RELATIONAL );
        builder.scan( getSubstitutionTable( statement, node.getCollection().getTable().getTableId(), columns.get( 0 ).id, adapterId ) );
        builder.project( node.getCluster().getRexBuilder().makeInputRef( node.getRowType(), 1 ) );
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), null, out.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, node.getRowType(), false ) );
        return builder;
    }


    private RoutedAlgBuilder handleDocumentOnGraph( DocumentScan alg, Statement statement, RoutedAlgBuilder builder ) {
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.GRAPH );
        builder.lpgScan( alg.getCollection().getTable().getTableId() );
        List<String> names = alg.getCollection().getQualifiedName();
        builder.lpgMatch( List.of( builder.lpgNodeMatch( List.of( names.get( names.size() - 1 ) ) ) ), List.of( "n" ) );
        AlgNode unrouted = builder.build();
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( routeGraph( builder, (AlgNode & LpgAlg) unrouted, statement ) ), null, out.replace( ModelTrait.DOCUMENT ), ModelTrait.GRAPH, ModelTrait.DOCUMENT, alg.getRowType(), true ) );
        return builder;
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
    public <T extends AlgNode & DocumentAlg> AlgNode routeDocument( RoutedAlgBuilder builder, T alg, Statement statement ) {
        throw new UnsupportedOperationException();
    }

}
