/*
 * Copyright 2019-2020 The Polypheny Project
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


import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedSchema;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.BuiltInMethod;


public class PolySchemaBuilder {

    private final static PolySchemaBuilder INSTANCE = new PolySchemaBuilder();

    public static PolySchemaBuilder getInstance() {
        return INSTANCE;
    }


    public AbstractPolyphenyDbSchema getCurrent( Transaction transaction ) {
        return update( transaction );
    }


    public AbstractPolyphenyDbSchema update( Transaction transaction ) {
        final AbstractPolyphenyDbSchema polyphenyDbSchema;
        final Schema schema = new RootSchema();
        if ( false ) {
            polyphenyDbSchema = new CachingPolyphenyDbSchema( null, schema, "" );
        } else {
            polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "" );
        }

        SchemaPlus rootSchema = polyphenyDbSchema.plus();

        //
        // Build logical schema
        CatalogCombinedDatabase combinedDatabase;
        try {
            combinedDatabase = transaction.getCatalog().getCombinedDatabase( 0 );
        } catch ( GenericCatalogException | UnknownSchemaException | UnknownTableException e ) {
            throw new RuntimeException( "Something went wrong while retrieving the current schema from the catalog.", e );
        }

        for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
            Map<String, LogicalTable> tableMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), combinedSchema.getSchema().name ).plus();
            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // TODO MV: This assumes that there are only "complete" placements of tables and no vertical portioning at all.
            //
            for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                List<String> columnNames = new LinkedList<>();
                final RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
                final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
                for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
                    columnNames.add( catalogColumn.name );
                    fieldInfo.add( catalogColumn.name, null, sqlType( typeFactory, catalogColumn ) ).nullable( catalogColumn.nullable );
                }
                List<Long> columnIds = new LinkedList<>();
                combinedTable.getColumns().forEach( c -> columnIds.add( c.id ) );
                LogicalTable table = new LogicalTable(
                        combinedTable.getTable().id,
                        combinedTable.getSchema().name,
                        combinedTable.getTable().name,
                        columnIds,
                        columnNames,
                        RelDataTypeImpl.proto( fieldInfo.build() ) );
                s.add( combinedTable.getTable().name, table );
                tableMap.put( combinedTable.getTable().name, table );
            }
            rootSchema.add( combinedSchema.getSchema().name, s );
            tableMap.forEach( rootSchema.getSubSchema( combinedSchema.getSchema().name )::add );
            if ( combinedDatabase.getDefaultSchema() != null && combinedSchema.getSchema().id == combinedDatabase.getDefaultSchema().id ) {
                tableMap.forEach( rootSchema::add );
            }
            s.polyphenyDbSchema().setSchema( new LogicalSchema( combinedSchema.getSchema().name, tableMap ) );
        }

        //
        // Build store schema
        try {
            List<CatalogStore> stores = transaction.getCatalog().getStores();
            for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
                for ( CatalogStore catalogStore : stores ) {
                    // Get list of tables on this store
                    Map<String, Set<Long>> tableIdsPerSchema = new HashMap<>();
                    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    // TODO: This assumes there are only full table placements !!!!!!!!!!!!!!!!!!
                    for ( CatalogColumnPlacement placement : transaction.getCatalog().getColumnPlacementsOnStoreAndSchema( catalogStore.id, combinedSchema.getSchema().id ) ) {
                        tableIdsPerSchema.putIfAbsent( placement.physicalSchemaName, new HashSet<>() );
                        tableIdsPerSchema.get( placement.physicalSchemaName ).add( placement.tableId );
                    }

                    for ( String physicalSchemaName : tableIdsPerSchema.keySet() ) {
                        Set<Long> tableIds = tableIdsPerSchema.get( physicalSchemaName );
                        Map<String, Table> physicalTables = new HashMap<>();
                        Store store = StoreManager.getInstance().getStore( catalogStore.id );
                        final String schemaName = buildStoreSchemaName( catalogStore.uniqueName, combinedSchema.getSchema().name, physicalSchemaName );
                        store.createNewSchema( transaction, rootSchema, schemaName );
                        SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, store.getCurrentSchema(), schemaName ).plus();
                        for ( long tableId : tableIds ) {
                            CatalogCombinedTable combinedTable = transaction.getCatalog().getCombinedTable( tableId );
                            Table table = store.createTableSchema( combinedTable );
                            physicalTables.put( combinedTable.getTable().name, table );
                            s.add( combinedTable.getTable().name, table );
                        }
                        rootSchema.add( schemaName, s );
                        physicalTables.forEach( rootSchema.getSubSchema( schemaName )::add );
                        rootSchema.getSubSchema( schemaName ).polyphenyDbSchema().setSchema( store.getCurrentSchema() );
                    }
                }
            }
        } catch ( GenericCatalogException | UnknownTableException e ) {
            throw new RuntimeException( "Something went wrong while retrieving the current schema from the catalog.", e );
        }

        return polyphenyDbSchema;
    }


    private RelDataType sqlType( RelDataTypeFactory typeFactory, CatalogColumn column ) {
        final PolyType polyType = PolyType.get( column.type.name() );
        if ( column.length != null && column.scale != null && polyType.allowsPrecScale( true, true ) ) {
            return typeFactory.createPolyType( polyType, column.length, column.scale );
        } else if ( column.length != null && polyType.allowsPrecNoScale() ) {
            return typeFactory.createPolyType( polyType, column.length );
        } else {
            assert polyType.allowsNoPrecNoScale();
            return typeFactory.createPolyType( polyType );
        }
    }


    public static String buildStoreSchemaName( String storeName, String logicalSchema, String physicalSchema ) {
        return storeName + "_" + logicalSchema + "_" + physicalSchema;
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
