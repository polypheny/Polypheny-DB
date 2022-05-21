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
import org.polypheny.db.algebra.logical.graph.LogicalGraph;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogNamespace;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.schema.impl.AbstractSchema;
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
        final AbstractPolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "", NamespaceType.RELATIONAL );

        SchemaPlus rootSchema = polyphenyDbSchema.plus();
        Catalog catalog = Catalog.getInstance();

        // Build logical schema
        CatalogDatabase catalogDatabase = catalog.getDatabase( 1 );
        for ( CatalogNamespace catalogNamespace : catalog.getSchemas( catalogDatabase.id, null ) ) {
            if ( catalogNamespace.namespaceType == NamespaceType.GRAPH ) {
                continue;
            }
            Map<String, LogicalTable> tableMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), catalogNamespace.name, catalogNamespace.namespaceType ).plus();
            for ( CatalogEntity catalogEntity : catalog.getTables( catalogNamespace.id, null ) ) {
                List<String> columnNames = new LinkedList<>();

                AlgDataType rowType;
                final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

                final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

                for ( CatalogColumn catalogColumn : catalog.getColumns( catalogEntity.id ) ) {
                    columnNames.add( catalogColumn.name );
                    fieldInfo.add( catalogColumn.name, null, catalogColumn.getAlgDataType( typeFactory ) );
                    fieldInfo.nullable( catalogColumn.nullable );
                }
                rowType = fieldInfo.build();

                List<Long> columnIds = new LinkedList<>();
                catalog.getColumns( catalogEntity.id ).forEach( c -> columnIds.add( c.id ) );
                if ( catalogEntity.entityType == EntityType.VIEW ) {
                    buildView( tableMap, s, catalogEntity, columnNames, fieldInfo, columnIds );
                } else if ( catalogEntity.entityType == EntityType.ENTITY || catalogEntity.entityType == EntityType.SOURCE || catalogEntity.entityType == EntityType.MATERIALIZED_VIEW ) {
                    buildEntity( catalog, catalogNamespace, tableMap, s, catalogEntity, columnNames, rowType, columnIds );
                } else {
                    throw new RuntimeException( "Unhandled table type: " + catalogEntity.entityType.name() );
                }
            }

            rootSchema.add( catalogNamespace.name, s, catalogNamespace.namespaceType );
            tableMap.forEach( rootSchema.getSubSchema( catalogNamespace.name )::add );
            if ( catalogDatabase.defaultSchemaId != null && catalogNamespace.id == catalogDatabase.defaultSchemaId ) {
                tableMap.forEach( rootSchema::add );
            }
            s.polyphenyDbSchema().setSchema( new LogicalSchema( catalogNamespace.name, tableMap ) );
        }

        // Build logical schema
        for ( CatalogGraphDatabase graph : catalog.getGraphs( catalogDatabase.id, null ) ) {
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), graph.name, NamespaceType.GRAPH ).plus();

            rootSchema.add( graph.name, s, NamespaceType.GRAPH );
            s.polyphenyDbSchema().setSchema( new LogicalGraph( graph.id ) );
        }

        List<CatalogAdapter> adapters = Catalog.getInstance().getAdapters();

        buildPhysicalTables( polyphenyDbSchema, rootSchema, catalog, catalogDatabase, adapters );

        buildPhysicalDocuments( polyphenyDbSchema, rootSchema, catalog, catalogDatabase, adapters );

        buildPhysicalGraphs( polyphenyDbSchema, rootSchema, catalog, catalogDatabase );

        isOutdated = false;
        return polyphenyDbSchema;
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
                SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentGraphNamespace(), schemaName, NamespaceType.GRAPH ).plus();
                rootSchema.add( schemaName, s, NamespaceType.GRAPH );

                rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( adapter.getCurrentGraphNamespace() );
            }
        }
    }


    private void buildPhysicalDocuments( AbstractPolyphenyDbSchema polyphenyDbSchema, SchemaPlus rootSchema, Catalog catalog, CatalogDatabase catalogDatabase, List<CatalogAdapter> adapters ) {
        // Build adapter schema (physical schema) DOCUMENT
        for ( CatalogNamespace catalogNamespace : catalog.getSchemas( catalogDatabase.id, null ).stream().filter( s -> s.namespaceType == NamespaceType.DOCUMENT ).collect( Collectors.toList() ) ) {
            for ( CatalogAdapter catalogAdapter : adapters ) {

                Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );

                /*if ( !adapter.getSupportedNamespaceTypes().contains( NamespaceType.DOCUMENT ) ) {
                    continue;
                }*/

                // Get list of documents on this adapter
                Map<String, Set<Long>> documentIdsPerSchema = new HashMap<>();
                for ( CatalogCollectionPlacement placement : Catalog.getInstance().getCollectionPlacements( catalogAdapter.id ) ) {
                    documentIdsPerSchema.putIfAbsent( placement.physicalNamespaceName, new HashSet<>() );
                    documentIdsPerSchema.get( placement.physicalNamespaceName ).add( placement.collectionId );
                }

                for ( String physicalSchemaName : documentIdsPerSchema.keySet() ) {
                    Set<Long> tableIds = documentIdsPerSchema.get( physicalSchemaName );

                    HashMap<String, Table> physicalTables = new HashMap<>();

                    final String schemaName = buildAdapterSchemaName( catalogAdapter.uniqueName, catalogNamespace.name, physicalSchemaName );

                    adapter.createNewSchema( rootSchema, schemaName );
                    SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentSchema(), schemaName, catalogNamespace.namespaceType ).plus();
                    for ( long tableId : tableIds ) {
                        CatalogEntity catalogEntity = catalog.getTable( tableId );

                        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( adapter.getAdapterId(), tableId );

                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( catalogNamespace.namespaceType == NamespaceType.GRAPH && catalogAdapter.supportedNamespaces.contains( catalogNamespace.namespaceType ) ) {
                                continue;
                            }

                            Table table = adapter.createDocumentSchema(
                                    catalogEntity,
                                    Catalog.getInstance().getColumnPlacementsOnAdapterSortedByPhysicalPosition( adapter.getAdapterId(), catalogEntity.id ),
                                    partitionPlacement );

                            physicalTables.put( catalog.getTable( tableId ).name + "_" + partitionPlacement.partitionId, table );

                            rootSchema.add( schemaName, s, catalogNamespace.namespaceType );
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
        for ( CatalogNamespace catalogNamespace : catalog.getSchemas( catalogDatabase.id, null ).stream().collect( Collectors.toList() ) ) {
            for ( CatalogAdapter catalogAdapter : adapters ) {
                // Get list of tables on this adapter
                Map<String, Set<Long>> tableIdsPerSchema = new HashMap<>();
                for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacementsOnAdapterAndSchema( catalogAdapter.id, catalogNamespace.id ) ) {
                    tableIdsPerSchema.putIfAbsent( placement.physicalSchemaName, new HashSet<>() );
                    tableIdsPerSchema.get( placement.physicalSchemaName ).add( placement.tableId );
                }

                for ( String physicalSchemaName : tableIdsPerSchema.keySet() ) {
                    Set<Long> tableIds = tableIdsPerSchema.get( physicalSchemaName );

                    HashMap<String, Table> physicalTables = new HashMap<>();
                    Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );

                    final String schemaName = buildAdapterSchemaName( catalogAdapter.uniqueName, catalogNamespace.name, physicalSchemaName );

                    adapter.createNewSchema( rootSchema, schemaName );
                    SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentSchema(), schemaName, catalogNamespace.namespaceType ).plus();
                    for ( long tableId : tableIds ) {
                        CatalogEntity catalogEntity = catalog.getTable( tableId );

                        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( adapter.getAdapterId(), tableId );

                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( catalogNamespace.namespaceType == NamespaceType.GRAPH && catalogAdapter.supportedNamespaces.contains( catalogNamespace.namespaceType ) ) {
                                continue;
                            }

                            Table table = adapter.createTableSchema(
                                    catalogEntity,
                                    Catalog.getInstance().getColumnPlacementsOnAdapterSortedByPhysicalPosition( adapter.getAdapterId(), catalogEntity.id ),
                                    partitionPlacement );

                            physicalTables.put( catalog.getTable( tableId ).name + "_" + partitionPlacement.partitionId, table );

                            rootSchema.add( schemaName, s, catalogNamespace.namespaceType );
                            physicalTables.forEach( rootSchema.getSubSchema( schemaName )::add );
                            rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( adapter.getCurrentSchema() );
                        }
                    }
                }
            }
        }
    }


    private void buildView( Map<String, LogicalTable> tableMap, SchemaPlus s, CatalogEntity catalogEntity, List<String> columnNames, Builder fieldInfo, List<Long> columnIds ) {
        LogicalView view = new LogicalView(
                catalogEntity.id,
                catalogEntity.getNamespaceName(),
                catalogEntity.name,
                columnIds,
                columnNames,
                AlgDataTypeImpl.proto( fieldInfo.build() ) );
        s.add( catalogEntity.name, view );
        tableMap.put( catalogEntity.name, view );
    }


    private void buildEntity( Catalog catalog, CatalogNamespace catalogNamespace, Map<String, LogicalTable> tableMap, SchemaPlus s, CatalogEntity catalogEntity, List<String> columnNames, AlgDataType rowType, List<Long> columnIds ) {
        LogicalTable table;
        if ( catalogNamespace.namespaceType == NamespaceType.RELATIONAL ) {
            table = new LogicalTable(
                    catalogEntity.id,
                    catalogEntity.getNamespaceName(),
                    catalogEntity.name,
                    columnIds,
                    columnNames,
                    AlgDataTypeImpl.proto( rowType ),
                    catalogNamespace.namespaceType );
            if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
                table.getConstraintIds()
                        .addAll( catalog.getForeignKeys( catalogEntity.id ).stream()
                                .filter( f -> f.enforcementTime == EnforcementTime.ON_COMMIT )
                                .map( f -> f.referencedKeyTableId )
                                .collect( Collectors.toList() ) );
                table.getConstraintIds()
                        .addAll( catalog.getExportedKeys( catalogEntity.id ).stream()
                                .filter( f -> f.enforcementTime == EnforcementTime.ON_COMMIT )
                                .map( f -> f.referencedKeyTableId )
                                .collect( Collectors.toList() ) );
            }
        } else if ( catalogNamespace.namespaceType == NamespaceType.DOCUMENT ) {
            table = new LogicalDocument(
                    catalogEntity.id,
                    catalogEntity.getNamespaceName(),
                    catalogEntity.name,
                    columnIds,
                    columnNames,
                    AlgDataTypeImpl.proto( rowType ),
                    catalogNamespace.namespaceType );
        } else {
            throw new RuntimeException( "Model is not supported" );
        }

        s.add( catalogEntity.name, table );
        tableMap.put( catalogEntity.name, table );
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
