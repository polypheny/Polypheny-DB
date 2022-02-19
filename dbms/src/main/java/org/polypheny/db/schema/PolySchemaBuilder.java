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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
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
        final AbstractPolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "", SchemaType.RELATIONAL );

        SchemaPlus rootSchema = polyphenyDbSchema.plus();
        Catalog catalog = Catalog.getInstance();

        // Build logical schema
        CatalogDatabase catalogDatabase = catalog.getDatabase( 1 );
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ) ) {
            Map<String, LogicalTable> tableMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), catalogSchema.name, catalogSchema.schemaType ).plus();
            for ( CatalogTable catalogTable : catalog.getTables( catalogSchema.id, null ) ) {
                List<String> columnNames = new LinkedList<>();

                AlgDataType rowType;
                final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

                final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

                for ( CatalogColumn catalogColumn : catalog.getColumns( catalogTable.id ) ) {
                    columnNames.add( catalogColumn.name );
                    fieldInfo.add( catalogColumn.name, null, catalogColumn.getAlgDataType( typeFactory ) );
                    fieldInfo.nullable( catalogColumn.nullable );
                }
                rowType = fieldInfo.build();

                List<Long> columnIds = new LinkedList<>();
                catalog.getColumns( catalogTable.id ).forEach( c -> columnIds.add( c.id ) );
                if ( catalogTable.tableType == TableType.VIEW ) {
                    LogicalView view = new LogicalView(
                            catalogTable.id,
                            catalogTable.getSchemaName(),
                            catalogTable.name,
                            columnIds,
                            columnNames,
                            AlgDataTypeImpl.proto( fieldInfo.build() ) );
                    s.add( catalogTable.name, view );
                    tableMap.put( catalogTable.name, view );
                } else if ( catalogTable.tableType == TableType.TABLE || catalogTable.tableType == TableType.SOURCE || catalogTable.tableType == TableType.MATERIALIZED_VIEW ) {
                    LogicalTable table = new LogicalTable(
                            catalogTable.id,
                            catalogTable.getSchemaName(),
                            catalogTable.name,
                            columnIds,
                            columnNames,
                            AlgDataTypeImpl.proto( rowType ),
                            catalogSchema.schemaType );
                    s.add( catalogTable.name, table );
                    tableMap.put( catalogTable.name, table );
                } else {
                    throw new RuntimeException( "Unhandled table type: " + catalogTable.tableType.name() );
                }
            }

            rootSchema.add( catalogSchema.name, s, catalogSchema.schemaType );
            tableMap.forEach( rootSchema.getSubSchema( catalogSchema.name )::add );
            if ( catalogDatabase.defaultSchemaId != null && catalogSchema.id == catalogDatabase.defaultSchemaId ) {
                tableMap.forEach( rootSchema::add );
            }
            s.polyphenyDbSchema().setSchema( new LogicalSchema( catalogSchema.name, tableMap ) );
        }

        // Build adapter schema (physical schema)
        List<CatalogAdapter> adapters = Catalog.getInstance().getAdapters();
        for ( CatalogSchema catalogSchema : catalog.getSchemas( catalogDatabase.id, null ) ) {
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
                    SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, adapter.getCurrentSchema(), schemaName, catalogSchema.schemaType ).plus();
                    for ( long tableId : tableIds ) {
                        CatalogTable catalogTable = catalog.getTable( tableId );

                        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( adapter.getAdapterId(), tableId );

                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            Table table = adapter.createTableSchema(
                                    catalogTable,
                                    Catalog.getInstance().getColumnPlacementsOnAdapterSortedByPhysicalPosition( adapter.getAdapterId(), catalogTable.id ),
                                    partitionPlacement );

                            physicalTables.put( catalog.getTable( tableId ).name + "_" + partitionPlacement.partitionId, table );

                            rootSchema.add( schemaName, s, catalogSchema.schemaType );
                            physicalTables.forEach( rootSchema.getSubSchema( schemaName )::add );
                            rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( adapter.getCurrentSchema() );
                        }
                    }
                }
            }
        }
        isOutdated = false;
        return polyphenyDbSchema;
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
