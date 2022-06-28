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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
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
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.TranslatableGraph;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;


/**
 * Base Router for all routers including DML, DQL and Cached plans.
 */
@Slf4j
public abstract class BaseRouter {

    public static final Cache<Integer, AlgNode> joinedScanCache = CacheBuilder.newBuilder()
            .maximumSize( RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.getInteger() )
            .build();

    final static Catalog catalog = Catalog.getInstance();


    static {
        RuntimeConfig.JOINED_TABLE_SCAN_CACHE_SIZE.setRequiresRestart( true );
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
            long partitionId = (long) partitionToPlacement.getKey();
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
                        ccp.tableId,
                        ccp.adapterUniqueName,
                        ccp.getLogicalSchemaName(),
                        ccp.getLogicalTableName(),
                        ccp.physicalSchemaName,
                        cpp.physicalTableName,
                        cpp.partitionId );
                // Final project
                ArrayList<RexNode> rexNodes = new ArrayList<>();
                List<CatalogColumn> placementList = currentPlacements.stream()
                        .map( col -> catalog.getField( col.columnId ) )
                        .sorted( Comparator.comparingInt( col -> col.position ) )
                        .collect( Collectors.toList() );
                for ( CatalogColumn catalogColumn : placementList ) {
                    rexNodes.add( builder.field( catalogColumn.name ) );
                }
                builder.project( rexNodes );

            } else if ( placementsByAdapter.size() > 1 ) {
                // We need to join placements on different adapters

                // Get primary key
                long pkid = catalog.getTable( currentPlacements.get( 0 ).tableId ).primaryKey;
                List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
                List<CatalogColumn> pkColumns = new LinkedList<>();
                for ( long pkColumnId : pkColumnIds ) {
                    pkColumns.add( catalog.getField( pkColumnId ) );
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
                ArrayList<RexNode> rexNodes = new ArrayList<>();
                List<CatalogColumn> placementList = currentPlacements.stream()
                        .map( col -> catalog.getField( col.columnId ) )
                        .sorted( Comparator.comparingInt( col -> col.position ) )
                        .collect( Collectors.toList() );
                for ( CatalogColumn catalogColumn : placementList ) {
                    rexNodes.add( builder.field( catalogColumn.name ) );
                }
                builder.project( rexNodes );
            } else {
                throw new RuntimeException( "The table '" + currentPlacements.get( 0 ).getLogicalTableName() + "' seems to have no placement. This should not happen!" );
            }
        }

        builder.union( true, placements.size() );

        AlgNode node = builder.build();
        if ( RuntimeConfig.JOINED_TABLE_SCAN_CACHE.getBoolean() ) {
            joinedScanCache.put( placements.hashCode(), node );
        }
        return node;
    }


    public AlgNode handleGraphScan( LogicalGraphScan alg, Statement statement ) {
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();

        Catalog catalog = Catalog.getInstance();
        CatalogGraphDatabase catalogGraph = catalog.getGraph( alg.getGraph().getId() );
        for ( int adapterId : catalogGraph.placements ) {
            CatalogAdapter adapter = catalog.getAdapter( adapterId );
            CatalogGraphPlacement graphPlacement = catalog.getGraphPlacement( catalogGraph.id, adapterId );
            String name = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, catalogGraph.name, graphPlacement.physicalName );

            Graph graph = reader.getGraph( name );

            if ( !(graph instanceof TranslatableGraph) ) {
                // needs substitution later on
                return getRelationalScan( alg, statement );
            }

            return new LogicalGraphScan( alg.getCluster(), alg.getTraitSet(), (TranslatableGraph) graph, alg.getRowType() );


        }
        // substituted on optimization
        return alg;
    }


    public AlgNode getRelationalScan( LogicalGraphScan alg, Statement statement ) {
        CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( alg.getGraph().getId() );

        PreparingTable nodesTable = getSubstitutionTable( statement, mapping.nodesId, mapping.idNodeId );
        PreparingTable nodePropertiesTable = getSubstitutionTable( statement, mapping.nodesPropertyId, mapping.idNodesPropertyId );
        PreparingTable edgesTable = getSubstitutionTable( statement, mapping.edgesId, mapping.idEdgeId );
        ;
        PreparingTable edgePropertiesTable = getSubstitutionTable( statement, mapping.edgesPropertyId, mapping.idEdgesPropertyId );

        AlgNode node = buildSubstitutionJoin( alg, nodesTable, nodePropertiesTable );

        AlgNode edge = buildSubstitutionJoin( alg, edgesTable, edgePropertiesTable );

        return LogicalTransformer.create( List.of( node, edge ), alg.getTraitSet().replace( ModelTrait.RELATIONAL ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, alg.getRowType() );

    }


    protected PreparingTable getSubstitutionTable( Statement statement, long tableId, long columnId ) {
        CatalogEntity nodes = Catalog.getInstance().getTable( tableId );
        List<CatalogColumnPlacement> placement = Catalog.getInstance().getColumnPlacement( columnId );
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.get( 0 ).adapterUniqueName,
                        nodes.getNamespaceName(),
                        placement.get( 0 ).physicalSchemaName
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


    protected RoutedAlgBuilder handleDocumentScan( DocumentScan alg, Statement statement, RoutedAlgBuilder builder, LogicalQueryInformation queryInformation ) {
        Catalog catalog = Catalog.getInstance();
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();

        if ( alg.getCollection().getTable().getSchemaType() != NamespaceType.DOCUMENT ) {
            return handleTransformerDocScan( alg, statement, builder, queryInformation );
        }

        CatalogCollection collection = catalog.getCollection( alg.getCollection().getTable().getTableId() );

        for ( Integer adapterId : collection.placements ) {
            CatalogAdapter adapter = catalog.getAdapter( adapterId );
            NamespaceType sourceModel = alg.getCollection().getTable().getSchemaType();

            if ( !adapter.getSupportedNamespaces().contains( sourceModel ) ) {
                // document on relational
                return handleDocumentOnRelational( alg, statement, builder );
            }
            CatalogCollectionPlacement placement = catalog.getCollectionPlacement( collection.id, adapterId );
            String namespaceName = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, collection.getNamespaceName(), placement.physicalNamespaceName );
            String collectionName = collection.name + "_" + placement.id;
            AlgOptTable collectionTable = reader.getDocument( List.of( namespaceName, collectionName ) );

            return builder.push( LogicalDocumentScan.create( alg.getCluster(), collectionTable ) );
        }

        throw new RuntimeException( "No placement found for the document." );
    }


    private RoutedAlgBuilder handleTransformerDocScan( DocumentScan alg, Statement statement, RoutedAlgBuilder builder, LogicalQueryInformation queryInformation ) {
        AlgNode scan = builder.scan( alg.getCollection() ).build();

        List<RoutedAlgBuilder> scans = ((AbstractDqlRouter) RoutingManager.getInstance().getRouters().get( 0 )).buildDql( scan, List.of( builder ), statement, alg.getCluster(), queryInformation );
        builder.push( scans.get( 0 ).build() );
        AlgTraitSet out = alg.getTraitSet().replace( ModelTrait.RELATIONAL );
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), out.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, alg.getRowType() ) );
        return builder;
    }


    @NotNull
    private RoutedAlgBuilder handleDocumentOnRelational( DocumentScan node, Statement statement, RoutedAlgBuilder builder ) {
        List<CatalogColumn> columns = Catalog.getInstance().getColumns( node.getCollection().getTable().getTableId() );
        AlgTraitSet out = node.getTraitSet().replace( ModelTrait.RELATIONAL );
        builder.scan( getSubstitutionTable( statement, node.getCollection().getTable().getTableId(), columns.get( 0 ).id ) );
        builder.project( node.getCluster().getRexBuilder().makeInputRef( node.getRowType(), 1 ) );
        builder.push( new LogicalTransformer( builder.getCluster(), List.of( builder.build() ), out.replace( ModelTrait.DOCUMENT ), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, node.getRowType() ) );
        return builder;
    }

}
