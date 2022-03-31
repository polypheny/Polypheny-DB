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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.LogicalDocuments;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.logical.graph.RelationalTransformable;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
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
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.rex.RexNode;
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


    protected RoutedAlgBuilder handleDocuments( LogicalDocuments node, RoutedAlgBuilder builder ) {
        return builder.documents( node.getDocumentTuples(), node.getRowType(), node.getTuples() );
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


    protected <T extends AlgNode & GraphAlg, G extends AlgNode & GraphAlg & RelationalTransformable> void attachMappingsIfNecessary( T input ) {
        if ( !(input instanceof RelationalTransformable) ) {
            return;
        }
        G alg = (G) input;
        CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( alg.getGraph().getId() );

        // --- nodes
        CatalogEntity nodes = Catalog.getInstance().getTable( mapping.nodesId );
        List<CatalogColumnPlacement> placement = Catalog.getInstance().getColumnPlacement( mapping.idNodeId );
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.get( 0 ).adapterUniqueName,
                        nodes.getNamespaceName(),
                        placement.get( 0 ).physicalSchemaName
                ),
                nodes.name + "_" + nodes.partitionProperty.partitionIds.get( 0 ) );

        PreparingTable node = alg.getCatalogReader().getTableForMember( qualifiedTableName );
        alg.setNodeTable( node );

        // --- node properties
        CatalogEntity nodeProperties = Catalog.getInstance().getTable( mapping.nodesPropertyId );
        placement = Catalog.getInstance().getColumnPlacement( mapping.idNodesPropertyId );
        qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.get( 0 ).adapterUniqueName,
                        nodeProperties.getNamespaceName(),
                        placement.get( 0 ).physicalSchemaName
                ),
                nodeProperties.name + "_" + nodeProperties.partitionProperty.partitionIds.get( 0 ) );

        PreparingTable nodeProperty = alg.getCatalogReader().getTableForMember( qualifiedTableName );
        alg.setNodePropertyTable( nodeProperty );
        // --- edge

        CatalogEntity edges = Catalog.getInstance().getTable( mapping.edgesId );
        placement = Catalog.getInstance().getColumnPlacement( mapping.idEdgeId );

        qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.get( 0 ).adapterUniqueName,
                        edges.getNamespaceName(),
                        placement.get( 0 ).physicalSchemaName
                ),
                edges.name + "_" + edges.partitionProperty.partitionIds.get( 0 ) );

        PreparingTable edge = alg.getCatalogReader().getTableForMember( qualifiedTableName );
        alg.setEdgeTable( edge );

        // -- edge property

        CatalogEntity edgeProperties = Catalog.getInstance().getTable( mapping.edgesPropertyId );
        placement = Catalog.getInstance().getColumnPlacement( mapping.idEdgesPropertyId );

        qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        placement.get( 0 ).adapterUniqueName,
                        edgeProperties.getNamespaceName(),
                        placement.get( 0 ).physicalSchemaName
                ),
                edgeProperties.name + "_" + edgeProperties.partitionProperty.partitionIds.get( 0 ) );

        PreparingTable edgeProperty = alg.getCatalogReader().getTableForMember( qualifiedTableName );
        alg.setEdgePropertyTable( edgeProperty );
    }


    protected AlgNode handleGraphScan( LogicalGraphScan alg, Statement statement ) {
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();

        CatalogGraphDatabase catalogGraph = Catalog.getInstance().getGraph( alg.getGraph().getId() );
        for ( int adapterId : catalogGraph.placements ) {
            CatalogAdapter adapter = Catalog.getInstance().getAdapter( adapterId );
            CatalogGraphPlacement graphPlacement = Catalog.getInstance().getGraphPlacement( catalogGraph.id, adapterId );
            String name = PolySchemaBuilder.buildAdapterSchemaName( adapter.uniqueName, catalogGraph.name, graphPlacement.physicalName );

            Graph graph = reader.getGraph( name );

            if ( !(graph instanceof TranslatableGraph) ) {
                // needs substitution later on
                return alg;
            }

            return new LogicalGraphScan( alg.getCluster(), reader, alg.getTraitSet(), (TranslatableGraph) graph, alg.getRowType() );


        }
        // substituted on optimization
        return alg;
    }

}
