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

package org.polypheny.db.schema;


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.logical.lpg.LogicalGraph;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.BuiltInMethod;


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
        final Schema schema = new RootSchema();
        final AbstractPolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "", NamespaceType.RELATIONAL, false );

        SchemaPlus rootSchema = polyphenyDbSchema.plus();
        Catalog catalog = Catalog.getInstance();

        CatalogDatabase catalogDatabase = catalog.getDatabase( 1 );

        // Build logical namespaces
        buildRelationalLogical( polyphenyDbSchema, rootSchema, catalog, catalogDatabase );

        buildDocumentLogical( polyphenyDbSchema, rootSchema, catalog, catalogDatabase );

        buildGraphLogical( polyphenyDbSchema, rootSchema, catalog, catalogDatabase );

        // Build mapping structures

        // Build physical namespaces
        List<CatalogAdapter> adapters = Catalog.getInstance().getAdapters();

        buildPhysicalTables( polyphenyDbSchema, rootSchema, catalog, catalogDatabase, adapters );

        buildPhysicalDocuments( polyphenyDbSchema, rootSchema, catalog, catalogDatabase, adapters );

        buildPhysicalGraphs( polyphenyDbSchema, rootSchema, catalog, catalogDatabase );

        isOutdated = false;
        return polyphenyDbSchema;
    }


    private void buildGraphLogical( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase ) {
        for ( CatalogGraphDatabase graph : catalog.getGraphs( catalogDatabase.id, null ) ) {
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), graph.name, NamespaceType.GRAPH, graph.caseSensitive ).plus();

            rootSchema.add( graph.name, s, NamespaceType.GRAPH );
            s.polyphenyDbSchema().setSchema( new LogicalGraph( graph.id ) );
        }
    }


    private void buildRelationalLogical( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase ) {
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ) ) {
            if ( catalogSchema.namespaceType != NamespaceType.RELATIONAL ) {
                continue;
            }
            Map<String, LogicalTable> tableMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), catalogSchema.name, catalogSchema.namespaceType, catalogSchema.caseSensitive ).plus();
            for ( CatalogTable catalogTable : catalog.getTables( catalogSchema.id, null ) ) {
                List<String> columnNames = new LinkedList<>();

                AlgDataType rowType;
                final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

                final Builder fieldInfo = typeFactory.builder();

                for ( CatalogColumn catalogColumn : catalog.getColumns( catalogTable.id ) ) {
                    columnNames.add( catalogColumn.name );
                    fieldInfo.add( catalogColumn.name, null, catalogColumn.getAlgDataType( typeFactory ) );
                    fieldInfo.nullable( catalogColumn.nullable );
                }
                rowType = fieldInfo.build();

                List<Long> columnIds = new LinkedList<>();
                catalog.getColumns( catalogTable.id ).forEach( c -> columnIds.add( c.id ) );
                if ( catalogTable.entityType == EntityType.VIEW ) {
                    buildView( tableMap, s, catalogTable, columnNames, fieldInfo, columnIds );
                } else if ( catalogTable.entityType == EntityType.ENTITY || catalogTable.entityType == EntityType.SOURCE || catalogTable.entityType == EntityType.MATERIALIZED_VIEW ) {
                    buildEntity( catalog, catalogSchema, tableMap, s, catalogTable, columnNames, rowType, columnIds );
                } else {
                    throw new RuntimeException( "Unhandled table type: " + catalogTable.entityType.name() );
                }
            }

            rootSchema.add( catalogSchema.name, s, catalogSchema.namespaceType );
            tableMap.forEach( rootSchema.getSubSchema( catalogSchema.name )::add );
            if ( catalogDatabase.defaultNamespaceId != null && catalogSchema.id == catalogDatabase.defaultNamespaceId ) {
                tableMap.forEach( rootSchema::add );
            }
            s.polyphenyDbSchema().setSchema( new LogicalSchema( catalogSchema.name, tableMap, new HashMap<>() ) );
        }
    }


    private void buildDocumentLogical( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase ) {
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ) ) {
            if ( catalogSchema.namespaceType != NamespaceType.DOCUMENT ) {
                continue;
            }
            Map<String, LogicalTable> collectionMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), catalogSchema.name, catalogSchema.namespaceType, catalogSchema.caseSensitive ).plus();
            for ( CatalogCollection catalogEntity : catalog.getCollections( catalogSchema.id, null ) ) {
                List<String> columnNames = new LinkedList<>();

                final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

                final Builder fieldInfo = typeFactory.builder();

                columnNames.add( "d" );
                fieldInfo.add( "d", null, typeFactory.createPolyType( PolyType.DOCUMENT ) );
                fieldInfo.nullable( false );

                List<Long> columnIds = new LinkedList<>();
                catalog.getColumns( catalogEntity.id ).forEach( c -> columnIds.add( c.id ) );
                LogicalTable entity;
                if ( catalogEntity.entityType == EntityType.VIEW ) {
                    entity = new LogicalRelView(
                            catalogEntity.id,
                            catalogEntity.getNamespaceName(),
                            catalogEntity.name,
                            columnIds,
                            columnNames,
                            AlgDataTypeImpl.proto( fieldInfo.build() ) );

                } else if ( catalogEntity.entityType == EntityType.ENTITY || catalogEntity.entityType == EntityType.SOURCE || catalogEntity.entityType == EntityType.MATERIALIZED_VIEW ) {
                    entity = new LogicalCollection(
                            catalogEntity.id,
                            catalogEntity.getNamespaceName(),
                            catalogEntity.name,
                            AlgDataTypeImpl.proto( fieldInfo.build() ) );
                } else {
                    throw new RuntimeException( "Unhandled table type: " + catalogEntity.entityType.name() );
                }

                s.add( catalogEntity.name, entity );
                collectionMap.put( catalogEntity.name, entity );
            }

            rootSchema.add( catalogSchema.name, s, catalogSchema.namespaceType );
            collectionMap.forEach( rootSchema.getSubSchema( catalogSchema.name )::add );
            if ( catalogDatabase.defaultNamespaceId != null && catalogSchema.id == catalogDatabase.defaultNamespaceId ) {
                collectionMap.forEach( rootSchema::add );
            }
            PolyphenyDbSchema schema = s.polyphenyDbSchema().getSubSchema( catalogSchema.name, catalogSchema.caseSensitive );
            if ( schema != null ) {
                LogicalSchema logicalSchema = new LogicalSchema( catalogSchema.name, ((LogicalSchema) schema.getSchema()).getTableMap(), collectionMap );
                s.polyphenyDbSchema().setSchema( logicalSchema );
            } else {
                s.polyphenyDbSchema().setSchema( new LogicalSchema( catalogSchema.name, new HashMap<>(), collectionMap ) );
            }

        }
    }


    private void buildPhysicalGraphs( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase ) {
        // Build adapter schema (physical schema) GRAPH
        for ( CatalogGraphDatabase graph : catalog.getGraphs( catalogDatabase.id, null ) ) {
            for ( int adapterId : graph.placements ) {

                CatalogGraphPlacement placement = catalog.getGraphPlacement( graph.id, adapterId );
                Adapter adapter = AdapterManager.getInstance().getAdapter( adapterId );

                if ( !adapter.getSupportedNamespaceTypes().contains( NamespaceType.GRAPH ) ) {
                    continue;
                }

                final String schemaName = buildAdapterSchemaName( adapter.getUniqueName(), graph.name, placement.physicalName );

                adapter.createGraphNamespace( rootSchema, schemaName, graph.id );
                SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentGraphNamespace(), schemaName, NamespaceType.GRAPH, graph.caseSensitive ).plus();
                rootSchema.add( schemaName, s, NamespaceType.GRAPH );

                rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( adapter.getCurrentGraphNamespace() );
            }
        }
    }


    private void buildPhysicalDocuments( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase, List<CatalogAdapter> adapters ) {
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

                    HashMap<String, Table> physicalTables = new HashMap<>();

                    final String schemaName = buildAdapterSchemaName( catalogAdapter.uniqueName, catalogSchema.name, physicalSchemaName );

                    adapter.createNewSchema( rootSchema, schemaName );
                    SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentSchema(), schemaName, catalogSchema.namespaceType, catalogSchema.caseSensitive ).plus();
                    for ( long collectionId : collectionIds ) {
                        CatalogCollection catalogCollection = catalog.getCollection( collectionId );

                        for ( CatalogCollectionPlacement partitionPlacement : catalogCollection.placements.stream().map( p -> Catalog.getInstance().getCollectionPlacement( collectionId, adapter.getAdapterId() ) ).collect( Collectors.toList() ) ) {
                            if ( catalogSchema.namespaceType != NamespaceType.DOCUMENT && catalogAdapter.getSupportedNamespaces().contains( catalogSchema.namespaceType ) ) {
                                continue;
                            }

                            Table table = adapter.createDocumentSchema( catalogCollection, partitionPlacement );

                            physicalTables.put( catalog.getCollection( collectionId ).name + "_" + partitionPlacement.id, table );

                            rootSchema.add( schemaName, s, catalogSchema.namespaceType );
                            physicalTables.forEach( rootSchema.getSubSchema( schemaName )::add );
                            rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( adapter.getCurrentSchema() );
                        }
                    }
                }
            }
        }
    }


    private void buildPhysicalTables( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase, List<CatalogAdapter> adapters ) {
        // Build adapter schema (physical schema) RELATIONAL
        for ( CatalogSchema catalogSchema : new ArrayList<>( catalog.getSchemas( catalogDatabase.id, null ) ) ) {
            for ( CatalogAdapter catalogAdapter : adapters ) {
                // Get list of tables on this adapter
                Map<String, Set<Long>> tableIdsPerSchema = new HashMap<>();
                for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacementsOnAdapterAndSchema( catalogAdapter.id, catalogSchema.id ) ) {
                    tableIdsPerSchema.putIfAbsent( placement.physicalSchemaName, new HashSet<>() );
                    tableIdsPerSchema.get( placement.physicalSchemaName ).add( placement.tableId );
                }

                for ( String physicalSchemaName : tableIdsPerSchema.keySet() ) {
                    Set<Long> tableIds = tableIdsPerSchema.get( physicalSchemaName );

                    HashMap<String, Table> physicalTables = new HashMap<>();
                    Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );

                    final String schemaName = buildAdapterSchemaName( catalogAdapter.uniqueName, catalogSchema.name, physicalSchemaName );

                    adapter.createNewSchema( rootSchema, schemaName );
                    SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentSchema(), schemaName, catalogSchema.namespaceType, catalogSchema.caseSensitive ).plus();
                    for ( long tableId : tableIds ) {
                        CatalogTable catalogTable = catalog.getTable( tableId );

                        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( adapter.getAdapterId(), tableId );

                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( catalogSchema.namespaceType != NamespaceType.RELATIONAL && catalogAdapter.getSupportedNamespaces().contains( catalogSchema.namespaceType ) ) {
                                continue;
                            }

                            Table table = adapter.createTableSchema(
                                    catalogTable,
                                    Catalog.getInstance().getColumnPlacementsOnAdapterSortedByPhysicalPosition( adapter.getAdapterId(), catalogTable.id ),
                                    partitionPlacement );

                            physicalTables.put( catalog.getTable( tableId ).name + "_" + partitionPlacement.partitionId, table );

                            rootSchema.add( schemaName, s, catalogSchema.namespaceType );
                            physicalTables.forEach( rootSchema.getSubSchema( schemaName )::add );
                            rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( adapter.getCurrentSchema() );
                        }
                    }
                }
            }
        }
    }


    private void buildView( Map<String, LogicalTable> tableMap, SchemaPlus s, CatalogTable catalogTable, List<String> columnNames, Builder fieldInfo, List<Long> columnIds ) {
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


    private void buildEntity( Catalog catalog, CatalogSchema catalogSchema, Map<String, LogicalTable> tableMap, SchemaPlus s, CatalogTable catalogTable, List<String> columnNames, AlgDataType rowType, List<Long> columnIds ) {
        LogicalTable table;
        if ( catalogSchema.namespaceType == NamespaceType.RELATIONAL ) {
            table = new LogicalTable(
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
    private static class RootSchema extends AbstractSchema {

        RootSchema() {
            super();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method );
        }

    }

}
