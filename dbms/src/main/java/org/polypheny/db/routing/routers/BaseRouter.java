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
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogNamespace;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.refactor.TranslatableEntity;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Pair;


/**
 * Base Router for all routers including DML, DQL and Cached plans.
 */
@Slf4j
public abstract class BaseRouter implements Router {

    public static final Cache<Integer, AlgNode> joinedScanCache = CacheBuilder.newBuilder()
            .maximumSize( RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.getInteger() )
            .build();

    final static Snapshot snapshot = Catalog.getInstance().getSnapshot();


    static {
        RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.setRequiresRestart( true );
    }


    /**
     * Execute the table scan on the first placement of a table
     */
    protected static Map<Long, List<CatalogColumnPlacement>> selectPlacement( LogicalTable table ) {
        // Find the adapter with the most column placements
        long adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Long, ImmutableList<Long>> entry : Catalog.getInstance().getSnapshot().getAllocSnapshot().getColumnPlacementsByAdapter( table.id ).entrySet() ) {
            if ( entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        // Take the adapter with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( LogicalColumn column : snapshot.getRelSnapshot( table.namespaceId ).getColumns( table.id ) ) {
            if ( snapshot.getAllocSnapshot().getDataPlacement( adapterIdWithMostPlacements, table.id ).columnPlacementsOnAdapter.contains( column.id ) ) {
                placementList.add( snapshot.getAllocSnapshot().getColumnPlacements( column.id ).get( 0 ) );
            } else {
                placementList.add( snapshot.getAllocSnapshot().getColumnPlacements( column.id ).get( 0 ) );
            }
        }

        return new HashMap<>() {{
            PartitionProperty property = snapshot.getAllocSnapshot().getPartitionProperty( table.id );
            put( property.partitionIds.get( 0 ), placementList );
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


    public RoutedAlgBuilder handleScan(
            RoutedAlgBuilder builder,
            Statement statement,
            long partitionId ) {

        PhysicalEntity physical = snapshot.getPhysicalSnapshot().getPhysicalTable( partitionId );
        AlgNode node = builder.scan( physical ).build();

        builder.push( node );

        if ( physical.namespaceType == NamespaceType.DOCUMENT
                && node.getRowType().getFieldCount() == 1
                && node.getRowType().getFieldList().get( 0 ).getName().equals( "d" )
                && node.getRowType().getFieldList().get( 0 ).getType().getPolyType() == PolyType.DOCUMENT ) {
            // relational on document -> expand document field into _id_ & _data_
            AlgNode scan = builder.build();
            builder.push( scan );
            AlgDataType type = getDocumentRowType();
            builder.documentProject( addDocumentNodes( scan.getRowType(), scan.getCluster().getRexBuilder(), true ), List.of( "_id_", "_data_" ) );

            builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), List.of( "_id_, _data_" ), scan.getTraitSet(), ModelTrait.DOCUMENT, ModelTrait.RELATIONAL, type, false ) );
        }

        return builder;
    }


    private AlgDataType getDocumentRowType() {
        // label table for cross model queries
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        final Builder fieldInfo = typeFactory.builder();
        fieldInfo.add( new AlgDataTypeFieldImpl( "_id_", 0, typeFactory.createPolyType( PolyType.VARCHAR, 255 ) ) );
        fieldInfo.add( new AlgDataTypeFieldImpl( "_data_", 1, typeFactory.createPolyType( PolyType.VARCHAR, 2064 ) ) );

        return fieldInfo.build();
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
            Map<Long, List<CatalogColumnPlacement>> placementsByAdapter = new HashMap<>();
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

                builder = handleScan(
                        builder,
                        statement,
                        partitionId );
                // Final project
                buildFinalProject( builder, currentPlacements );

            } else if ( placementsByAdapter.size() > 1 ) {
                // We need to join placements on different adapters

                // Get primary key
                LogicalRelSnapshot relSnapshot = snapshot.getRelSnapshot( currentPlacements.get( 0 ).namespaceId );
                long pkid = relSnapshot.getTable( currentPlacements.get( 0 ).tableId ).primaryKey;
                List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).columnIds;
                List<LogicalColumn> pkColumns = new LinkedList<>();
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( relSnapshot.getColumn( pkColumnId ) );
                }

                // Add primary key
                for ( Entry<Long, List<CatalogColumnPlacement>> entry : placementsByAdapter.entrySet() ) {
                    for ( LogicalColumn pkColumn : pkColumns ) {
                        CatalogColumnPlacement pkPlacement = Catalog.getInstance().getSnapshot().getAllocSnapshot().getColumnPlacements( pkColumn.id ).get( 0 );
                        if ( !entry.getValue().contains( pkPlacement ) ) {
                            entry.getValue().add( pkPlacement );
                        }
                    }
                }

                Deque<String> queue = new LinkedList<>();
                boolean first = true;
                for ( List<CatalogColumnPlacement> ccps : placementsByAdapter.values() ) {
                    CatalogColumnPlacement ccp = ccps.get( 0 );
                    CatalogPartitionPlacement cpp = Catalog.getInstance().getSnapshot().getAllocSnapshot().getPartitionPlacement( ccp.adapterId, partitionId );

                    handleScan(
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
        }

        builder.union( true, placements.size() );

        AlgNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedScanCache.put( placements.hashCode(), node );
        }

        CatalogColumnPlacement placement = new ArrayList<>( placements.values() ).get( 0 ).get( 0 );
        // todo dl: remove after RowType refactor
        if ( snapshot.getNamespace( placement.namespaceId ).namespaceType == NamespaceType.DOCUMENT ) {
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
        List<LogicalColumn> placementList = currentPlacements.stream()
                .map( col -> snapshot.getRelSnapshot( currentPlacements.get( 0 ).namespaceId ).getColumn( col.columnId ) )
                .sorted( Comparator.comparingInt( col -> col.position ) )
                .collect( Collectors.toList() );
        for ( LogicalColumn logicalColumn : placementList ) {
            rexNodes.add( builder.field( logicalColumn.name ) );
        }
        builder.project( rexNodes );
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

        List<Long> placements = snapshot.getAllocSnapshot().getGraphPlacements( catalogGraph.id ).stream().map( p -> p.adapterId ).collect( Collectors.toList() );
        if ( placementId != null ) {
            placements = List.of( placementId );
        }

        for ( long adapterId : placements ) {
            PhysicalGraph graph = snapshot.getPhysicalSnapshot().getPhysicalGraph( catalogGraph.id, adapterId );

            if ( !(graph instanceof TranslatableEntity) ) {
                // needs substitution later on
                scans.add( getRelationalScan( alg, adapterId, statement ) );
                continue;
            }

            // a native placement was used, we go with that
            return new LogicalLpgScan( alg.getCluster(), alg.getTraitSet(), graph, alg.getRowType() );
        }
        if ( scans.size() < 1 ) {
            throw new RuntimeException( "Error while routing graph query." );
        }

        // rather naive selection strategy
        return scans.get( 0 );
    }


    private AlgNode handleGraphOnRelational( LogicalLpgScan alg, CatalogNamespace namespace, Statement statement, Long placementId ) {
        AlgOptCluster cluster = alg.getCluster();
        List<LogicalTable> tables = snapshot.getRelSnapshot( namespace.id ).getTables( null );
        List<Pair<String, AlgNode>> scans = tables.stream()
                .map( t -> Pair.of( t.name, buildJoinedScan( statement, cluster, selectPlacement( t ) ) ) )
                .collect( Collectors.toList() );

        Builder infoBuilder = cluster.getTypeFactory().builder();
        infoBuilder.add( "g", null, PolyType.GRAPH );

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), alg.getTraitSet().replace( ModelTrait.GRAPH ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, infoBuilder.build(), true );
    }


    private AlgNode handleGraphOnDocument( LogicalLpgScan alg, CatalogNamespace namespace, Statement statement, Long placementId ) {
        AlgOptCluster cluster = alg.getCluster();
        List<LogicalCollection> collections = snapshot.getDocSnapshot( namespace.id ).getCollections( null );
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

        return new LogicalTransformer( cluster, Pair.right( scans ), Pair.left( scans ), alg.getTraitSet().replace( ModelTrait.GRAPH ), ModelTrait.DOCUMENT, ModelTrait.GRAPH, infoBuilder.build(), true );
    }


    public AlgNode getRelationalScan( LogicalLpgScan alg, long adapterId, Statement statement ) {
        /*CatalogGraphMapping mapping = Catalog.getInstance().getLogicalGraph( alg.entity.namespaceId ).getGraphMapping( alg.entity.id );

        PhysicalTable nodesTable = statement.getDataContext().getSnapshot().getTable( mapping.nodesId ).unwrap( PhysicalTable.class );
        PhysicalTable nodePropertiesTable = statement.getDataContext().getSnapshot().getTable( mapping.nodesPropertyId ).unwrap( PhysicalTable.class );
        PhysicalTable edgesTable = statement.getDataContext().getSnapshot().getTable( mapping.edgesId ).unwrap( PhysicalTable.class );
        PhysicalTable edgePropertiesTable = statement.getDataContext().getSnapshot().getTable( mapping.edgesPropertyId ).unwrap( PhysicalTable.class );

        AlgNode node = buildSubstitutionJoin( alg, nodesTable, nodePropertiesTable );

        AlgNode edge = buildSubstitutionJoin( alg, edgesTable, edgePropertiesTable );

        return LogicalTransformer.create( List.of( node, edge ), alg.getTraitSet().replace( ModelTrait.RELATIONAL ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, alg.getRowType() );
        */ // todo dl
        return null;
    }


    protected CatalogEntity getSubstitutionTable( Statement statement, long tableId, long columnId, long adapterId ) {
        /*LogicalTable nodes = Catalog.getInstance().getTable( tableId );
        CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacements( adapterId, columnId );
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.adapterUniqueName,
                        nodes.getNamespaceName(),
                        placement.physicalSchemaName
                ),
                nodes.name + "_" + nodes.partitionProperty.partitionIds.get( 0 ) );

        return statement.getDataContext().getSnapshot().getTable( qualifiedTableName );
        */ // todo dl
        return null;
    }


    protected AlgNode buildSubstitutionJoin( AlgNode alg, CatalogEntity nodesTable, CatalogEntity nodePropertiesTable ) {
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.RELATIONAL );
        LogicalRelScan nodes = new LogicalRelScan( alg.getCluster(), out, nodesTable );
        LogicalRelScan nodesProperty = new LogicalRelScan( alg.getCluster(), out, nodePropertiesTable );

        RexBuilder builder = alg.getCluster().getRexBuilder();

        RexNode nodeCondition = builder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                builder.makeInputRef( nodes.getRowType().getFieldList().get( 0 ).getType(), 0 ),
                builder.makeInputRef( nodesProperty.getRowType().getFieldList().get( 0 ).getType(), nodes.getRowType().getFieldList().size() ) );

        return new LogicalJoin( alg.getCluster(), out, nodes, nodesProperty, nodeCondition, Set.of(), JoinAlgType.LEFT, false, ImmutableList.of() );
    }


    protected RoutedAlgBuilder handleDocumentScan( DocumentScan<?> alg, Statement statement, RoutedAlgBuilder builder, Long adapterId ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        if ( alg.entity.namespaceType != NamespaceType.DOCUMENT ) {
            if ( alg.entity.namespaceType == NamespaceType.GRAPH ) {
                return handleDocumentOnGraph( alg, statement, builder );
            }

            return handleTransformerDocScan( alg, statement, builder );
        }

        LogicalCollection collection = alg.entity.unwrap( LogicalCollection.class );

        List<RoutedAlgBuilder> scans = new ArrayList<>();

        List<Long> placements = snapshot.getAllocSnapshot().getCollectionPlacements( collection.id ).stream().map( p -> p.adapterId ).collect( Collectors.toList() );
        if ( adapterId != null ) {
            placements = List.of( adapterId );
        }

        for ( Long placementId : placements ) {
            CatalogAdapter adapter = snapshot.getAdapter( placementId );
            NamespaceType sourceModel = collection.namespaceType;

            if ( !adapter.supportedNamespaces.contains( sourceModel ) ) {
                // document on relational
                scans.add( handleDocumentOnRelational( (DocumentScan<LogicalTable>) alg, placementId, statement, builder ) );
                continue;
            }
            // CatalogCollectionPlacement placement = catalog.getAllocDoc( alg.entity ).getCollectionPlacement( collection.id, placementId );
            // String namespaceName = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, collection.getNamespaceName(), placement.physicalNamespaceName );
            // String collectionName = collection.name + "_" + placement.id;
            PhysicalTable collectionTable = snapshot.getPhysicalSnapshot().getPhysicalTable( collection.id, adapterId );
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


    private RoutedAlgBuilder handleTransformerDocScan( DocumentScan<?> alg, Statement statement, RoutedAlgBuilder builder ) {
        /*AlgNode scan = buildJoinedScan( statement, alg.getCluster(), selectPlacement( alg.entity  ) );

        builder.push( scan );
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.RELATIONAL );
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), null, out.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, alg.getRowType(), false ) );

        return builder;
         */// todo dl
        return builder;
    }


    @NotNull
    private RoutedAlgBuilder handleDocumentOnRelational( DocumentScan<LogicalTable> node, Long adapterId, Statement statement, RoutedAlgBuilder builder ) {
        List<LogicalColumn> columns = statement.getTransaction().getSnapshot().getRelSnapshot( node.entity.namespaceId ).getColumns( node.entity.id );
        AlgTraitSet out = node.getTraitSet().replace( ModelTrait.RELATIONAL );
        CatalogEntity subTable = getSubstitutionTable( statement, node.entity.id, columns.get( 0 ).id, adapterId );
        builder.scan( subTable );
        builder.project( node.getCluster().getRexBuilder().makeInputRef( subTable.getRowType().getFieldList().get( 1 ).getType(), 1 ) );
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), null, out.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, node.getRowType(), false ) );
        return builder;
    }


    private RoutedAlgBuilder handleDocumentOnGraph( DocumentScan<?> alg, Statement statement, RoutedAlgBuilder builder ) {
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.GRAPH );
        builder.lpgScan( alg.entity.id );
        builder.lpgMatch( List.of( builder.lpgNodeMatch( List.of( alg.entity.name ) ) ), List.of( "n" ) );
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
