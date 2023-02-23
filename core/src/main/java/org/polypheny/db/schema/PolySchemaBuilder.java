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

package org.polypheny.db.schema;


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogEntityPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.impl.AbstractNamespace;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;


public class PolySchemaBuilder implements PropertyChangeListener {

    private final static PolySchemaBuilder INSTANCE = new PolySchemaBuilder();

    private AbstractPolyphenyDbSchema current;
    private boolean isOutdated = true;


    private PolySchemaBuilder() {
        Catalog.getInstance().addObserver( this );
    }


    public static PolySchemaBuilder getInstance() {
        return INSTANCE;
    }


    public AbstractPolyphenyDbSchema getCurrent() {
        if ( !RuntimeConfig.SCHEMA_CACHING.getBoolean() ) {
            return buildSchema();
        }
        if ( current == null || isOutdated ) {
            current = buildSchema();
        }
        return current;
    }


    private synchronized AbstractPolyphenyDbSchema buildSchema() {

        Catalog catalog = Catalog.getInstance();
        CatalogDatabase catalogDatabase = catalog.getDatabase( Catalog.defaultDatabaseId );

        // Build logical namespaces
        Map<Pair<Long, Long>, CatalogEntity> logicalRelational = buildRelationalLogical( catalog, catalogDatabase );

        Map<Pair<Long, Long>, CatalogEntity> logicalDocument = buildDocumentLogical( catalog, catalogDatabase );

        Map<Pair<Long, Long>, CatalogEntity> logicalGraph = buildGraphLogical( catalog, catalogDatabase );

        // Build mapping structures

        // Build physical namespaces
        List<CatalogAdapter> adapters = Catalog.getInstance().getAdapters();

        Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalRelational = buildPhysicalTables( catalog, catalogDatabase, adapters );

        Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalDocument = buildPhysicalDocuments( catalog, catalogDatabase, adapters );

        Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalGraph = buildPhysicalGraphs( catalog, catalogDatabase );

        isOutdated = false;
        return new SimplePolyphenyDbSchema( logicalRelational, logicalDocument, logicalGraph, physicalRelational, physicalDocument, physicalGraph );
    }


    private Map<Pair<Long, Long>, CatalogEntity> buildGraphLogical( Catalog catalog, CatalogDatabase catalogDatabase ) {
        return catalog.getGraphs( catalogDatabase.id, null ).stream().collect( Collectors.toMap( e -> Pair.of( e.id, e.id ), e -> e ) );
    }


    private Map<Pair<Long, Long>, CatalogEntity> buildRelationalLogical( Catalog catalog, CatalogDatabase catalogDatabase ) {
        Map<Pair<Long, Long>, CatalogEntity> entities = new HashMap<>();
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ) ) {
            if ( catalogSchema.namespaceType != NamespaceType.RELATIONAL ) {
                continue;
            }

            for ( CatalogTable catalogTable : catalog.getTables( catalogSchema.id, null ) ) {
                entities.put( Pair.of( catalogSchema.id, catalogTable.id ), catalogTable );
            }
        }
        return entities;
    }


    private Map<Pair<Long, Long>, CatalogEntity> buildDocumentLogical( Catalog catalog, CatalogDatabase catalogDatabase ) {
        Map<Pair<Long, Long>, CatalogEntity> entities = new HashMap<>();
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ) ) {
            if ( catalogSchema.namespaceType != NamespaceType.DOCUMENT ) {
                continue;
            }

            for ( CatalogCollection catalogEntity : catalog.getCollections( catalogSchema.id, null ) ) {
                entities.put( Pair.of( catalogSchema.id, catalogEntity.id ), catalogEntity );
            }
        }

        return entities;
    }


    private Map<Triple<Long, Long, Long>, CatalogEntityPlacement> buildPhysicalGraphs( Catalog catalog, CatalogDatabase catalogDatabase ) {
        Map<Triple<Long, Long, Long>, CatalogEntityPlacement> placements = new HashMap<>();
        // Build adapter schema (physical schema) GRAPH
        for ( CatalogGraphDatabase graph : catalog.getGraphs( catalogDatabase.id, null ) ) {
            for ( int adapterId : graph.placements ) {

                CatalogGraphPlacement placement = catalog.getGraphPlacement( graph.id, adapterId );
                Adapter adapter = AdapterManager.getInstance().getAdapter( adapterId );

                if ( !adapter.getSupportedNamespaceTypes().contains( NamespaceType.GRAPH ) ) {
                    continue;
                }

                //adapter.createGraphNamespace( rootSchema, schemaName, graph.id );

                placements.put( new Triple<>( graph.id, (long) adapter.getAdapterId(), graph.id ), placement );
            }
        }
        return placements;
    }


    private Map<Triple<Long, Long, Long>, CatalogEntityPlacement> buildPhysicalDocuments( Catalog catalog, CatalogDatabase catalogDatabase, List<CatalogAdapter> adapters ) {
        Map<Triple<Long, Long, Long>, CatalogEntityPlacement> placements = new HashMap<>();
        // Build adapter schema (physical schema) DOCUMENT
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ).stream().filter( s -> s.namespaceType == NamespaceType.DOCUMENT ).collect( Collectors.toList() ) ) {
            for ( CatalogAdapter catalogAdapter : adapters ) {

                Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );

                if ( !adapter.getSupportedNamespaceTypes().contains( NamespaceType.DOCUMENT ) ) {
                    continue;
                }

                // Get list of documents on this adapter
                Map<String, Set<Long>> documentIdsPerSchema = new HashMap<>();
                for ( CatalogCollectionPlacement placement : Catalog.getInstance().getCollectionPlacementsByAdapter( catalogAdapter.id ) ) {
                    documentIdsPerSchema.putIfAbsent( placement.physicalNamespaceName, new HashSet<>() );
                    documentIdsPerSchema.get( placement.physicalNamespaceName ).add( placement.collectionId );
                }

                for ( String physicalSchemaName : documentIdsPerSchema.keySet() ) {
                    Set<Long> collectionIds = documentIdsPerSchema.get( physicalSchemaName );

                    //adapter.createNewSchema( rootSchema, schemaName, catalogSchema.id );

                    for ( long collectionId : collectionIds ) {
                        CatalogCollection catalogCollection = catalog.getCollection( collectionId );

                        for ( CatalogCollectionPlacement partitionPlacement : catalogCollection.placements.stream().map( p -> catalog.getCollectionPlacement( collectionId, adapter.getAdapterId() ) ).collect( Collectors.toList() ) ) {
                            if ( catalogSchema.namespaceType != NamespaceType.DOCUMENT && catalogAdapter.getSupportedNamespaces().contains( catalogSchema.namespaceType ) ) {
                                continue;
                            }

                            //Entity entity = adapter.createDocumentSchema( catalogCollection, partitionPlacement );
                            placements.put( new Triple<>( catalogSchema.id, (long) catalogAdapter.id, catalogCollection.id ), partitionPlacement );
                        }
                    }
                }
            }
        }
        return placements;
    }


    private Map<Triple<Long, Long, Long>, CatalogEntityPlacement> buildPhysicalTables( Catalog catalog, CatalogDatabase catalogDatabase, List<CatalogAdapter> adapters ) {
        Map<Triple<Long, Long, Long>, CatalogEntityPlacement> placements = new HashMap<>();
        // Build adapter schema (physical schema) RELATIONAL
        for ( CatalogSchema catalogSchema : new ArrayList<>( catalog.getSchemas( catalogDatabase.id, null ) ) ) {
            for ( CatalogAdapter catalogAdapter : adapters ) {
                // Get list of tables on this adapter
                Map<Long, Set<Long>> tableIdsPerSchema = new HashMap<>();
                for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacementsOnAdapterAndSchema( catalogAdapter.id, catalogSchema.id ) ) {
                    tableIdsPerSchema.putIfAbsent( placement.namespaceId, new HashSet<>() );
                    tableIdsPerSchema.get( placement.namespaceId ).add( placement.tableId );
                }

                for ( Long namespaceId : tableIdsPerSchema.keySet() ) {
                    Set<Long> tableIds = tableIdsPerSchema.get( namespaceId );
                    //adapter.createNewSchema( rootSchema, schemaName, catalogSchema.id );
                    for ( long tableId : tableIds ) {
                        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( catalogAdapter.id, tableId );

                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( catalogSchema.namespaceType != NamespaceType.RELATIONAL && catalogAdapter.getSupportedNamespaces().contains( catalogSchema.namespaceType ) ) {
                                continue;
                            }

                            /*
                            Entity entity = adapter.createTableSchema(
                                    catalogTable,
                                    Catalog.getInstance().getColumnPlacementsOnAdapterSortedByPhysicalPosition( adapter.getAdapterId(), catalogTable.id ),
                                    partitionPlacement );

                             */
                            placements.put( new Triple<>( catalogSchema.id, (long) catalogAdapter.id, partitionPlacement.tableId ), partitionPlacement );
                        }
                    }
                }
            }
        }

        return placements;
    }


    private void buildView( Map<String, LogicalEntity> tableMap, SchemaPlus s, CatalogTable catalogTable, List<String> columnNames, Builder fieldInfo, List<Long> columnIds ) {
        LogicalRelView view = new LogicalRelView(
                catalogTable.id,
                catalogTable.getNamespaceName(),
                catalogTable.name,
                columnIds,
                columnNames,
                AlgDataTypeImpl.proto( fieldInfo.build() ) );
        s.add( catalogTable.name, view );
        tableMap.put( catalogTable.name, view );
    }


    private void buildEntity( Catalog catalog, CatalogSchema catalogSchema, Map<String, LogicalEntity> tableMap, SchemaPlus s, CatalogTable catalogTable, List<String> columnNames, AlgDataType rowType, List<Long> columnIds ) {
        LogicalEntity table;
        if ( catalogSchema.namespaceType == NamespaceType.RELATIONAL ) {
            table = new LogicalEntity(
                    catalogTable.id,
                    catalogTable.getNamespaceName(),
                    catalogTable.name,
                    columnIds,
                    columnNames,
                    AlgDataTypeImpl.proto( rowType ),
                    catalogSchema.namespaceType );
            if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
                table.getConstraintIds()
                        .addAll( catalog.getForeignKeys( catalogTable.id ).stream()
                                .filter( f -> f.enforcementTime == EnforcementTime.ON_COMMIT )
                                .map( f -> f.referencedKeyTableId )
                                .collect( Collectors.toList() ) );
                table.getConstraintIds()
                        .addAll( catalog.getExportedKeys( catalogTable.id ).stream()
                                .filter( f -> f.enforcementTime == EnforcementTime.ON_COMMIT )
                                .map( f -> f.referencedKeyTableId )
                                .collect( Collectors.toList() ) );
            }
        } else if ( catalogSchema.namespaceType == NamespaceType.DOCUMENT ) {
            table = new LogicalCollection(
                    catalogTable.id,
                    catalogTable.getNamespaceName(),
                    catalogTable.name,
                    AlgDataTypeImpl.proto( rowType )
            );
        } else {
            throw new RuntimeException( "Model is not supported" );
        }

        s.add( catalogTable.name, table );
        tableMap.put( catalogTable.name, table );
    }


    public static String buildAdapterSchemaName( String storeName, String logicalSchema, String physicalSchema ) {
        return storeName + "_" + logicalSchema + "_" + physicalSchema;
    }


    // Listens on changes to the catalog
    @Override
    public void propertyChange( PropertyChangeEvent evt ) {
        // Catalog changed, flag as outdated
        isOutdated = true;
    }


    /**
     * Schema that has no parents.
     */
    private static class RootSchema extends AbstractNamespace implements Schema {

        RootSchema() {
            super( -1L );
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method );
        }

    }


}
