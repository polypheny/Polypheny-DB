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

package org.polypheny.db.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.CatalogImpl;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.type.PolyType;


@Slf4j
public class CatalogTest {

    CatalogImpl catalog;


    @Before
    public void setup() {
        catalog = new CatalogImpl( "testDB", false, false, true );
        catalog.clear();
    }


    @After
    public void cleanup() {
        catalog.close();
    }


    @Test
    public void testInit() {
        assert (catalog != null);
    }


    @Test
    public void testLayout() throws UnknownDatabaseException, UnknownSchemaException, UnknownTableException, UnknownColumnException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );
        assertEquals( userId, user.id );
        assertEquals( "tester", user.name );

        long databaseId = catalog.addDatabase( "test_db", userId, user.name, 0, "test_schema" );
        CatalogDatabase database = catalog.getDatabase( "test_db" );
        assertEquals( databaseId, database.id );

        long schemaId = catalog.addSchema( "test_schema", databaseId, userId, SchemaType.RELATIONAL );
        CatalogSchema schema = catalog.getSchema( databaseId, "test_schema" );
        assertEquals( schemaId, schema.id );

        long tableId = catalog.addTable( "test_table", schemaId, userId, TableType.TABLE, true );
        CatalogTable table = catalog.getTable( schemaId, "test_table" );
        assertEquals( tableId, table.id );

        long columnId = catalog.addColumn( "test_column", tableId, 0, PolyType.BIGINT, null, null, null, null, null, false, null );
        CatalogColumn column = catalog.getColumn( tableId, "test_column" );
        assertEquals( columnId, column.id );
    }


    @Test
    public void testDatabase() {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        List<String> names = Arrays.asList( "database1", "database2", "database3" );
        List<Long> ids = new ArrayList<>();
        for ( String name : names ) {
            ids.add( catalog.addDatabase( name, userId, user.name, 0, "" ) );
        }

        assertEquals( catalog.getDatabases( null ).stream().map( d -> d.name ).collect( Collectors.toList() ), names );

        for ( Long id : ids ) {
            catalog.deleteDatabase( id );
        }

        assertEquals( catalog.getDatabases( null ), Collections.emptyList() );

    }


    @Test
    public void testSchema() throws UnknownSchemaException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );

        List<String> names = new ArrayList<>( Arrays.asList( "schema1", "schema2", "schema3" ) );
        List<Long> ids = new ArrayList<>();

        // test adding of schema

        for ( String name : names ) {
            ids.add( catalog.addSchema( name, databaseId, userId, SchemaType.RELATIONAL ) );
        }
        assertEquals( catalog.getSchemas( databaseId, null ).stream().map( s -> s.name ).collect( Collectors.toList() ), names );

        // test renaming of schema
        String replacedName = "newName";
        Long id = ids.get( 0 );
        catalog.renameSchema( id, replacedName );
        names.remove( 0 );
        names.add( 0, replacedName );

        assertEquals( catalog.getSchemas( databaseId, null ).stream().map( s -> s.name ).collect( Collectors.toList() ), names );

        // test changing owner of schema
        int newUserId = catalog.addUser( "newUser", "" );
        catalog.setSchemaOwner( id, newUserId );

        assertEquals( catalog.getSchema( databaseId, replacedName ).ownerId, newUserId );
    }


    @Test
    public void testTable() throws GenericCatalogException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );

        long schemaId = catalog.addSchema( "schema1", databaseId, userId, SchemaType.RELATIONAL );

        List<String> names = new ArrayList<>( Arrays.asList( "table1", "table2", "table3", "table4", "table5" ) );
        List<Long> ids = new ArrayList<>();

        for ( String name : names ) {
            ids.add( catalog.addTable( name, schemaId, userId, TableType.TABLE, true ) );
        }

        // test renaming table
        String newTable = "newTable";
        Long tableId = ids.get( 3 );

        names.remove( 3 );
        names.add( 3, newTable );

        catalog.renameTable( tableId, newTable );
        assertEquals( names, catalog.getTables( null, null, null ).stream().sorted().map( s -> s.name ).collect( Collectors.toList() ) );

        // test change owner
        String newUserName = "newUser";
        int newUserId = catalog.addUser( newUserName, "" );
        catalog.setTableOwner( tableId, newUserId );

        assertEquals( catalog.getTable( tableId ).ownerId, newUserId );
        assertEquals( catalog.getTable( tableId ).ownerName, newUserName );

        // test change primary
        List<String> columnNames = new ArrayList<>( Arrays.asList( "column1", "column2" ) );
        List<Long> columnIds = new ArrayList<>();
        int counter = 0;
        for ( String name : columnNames ) {
            columnIds.add( catalog.addColumn( name, tableId, counter++, PolyType.BIGINT, null, null, null, null, null, false, null ) );
        }

        Long columnId = columnIds.get( 0 );
        catalog.addPrimaryKey( tableId, new ArrayList<>( Collections.singleton( columnId ) ) );

        CatalogPrimaryKey key = catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey );
        assertEquals( key.columnIds.get( 0 ), columnId );

        //catalog.deletePrimaryKey( tableId );
        //assertNull( catalog.getTable( tableId ).primaryKey );

        catalog.addPrimaryKey( tableId, columnIds );
        key = catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey );
        assertEquals( key.columnIds, columnIds );

        catalog.deleteTable( tableId );
        ids.remove( tableId );

        List<Long> collect = catalog.getTables( schemaId, null ).stream().map( t -> t.id ).collect( Collectors.toList() );
        assertEquals( collect, ids );
    }


    @Test
    public void testColumn() throws GenericCatalogException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );

        long schemaId = catalog.addSchema( "schema1", databaseId, userId, SchemaType.RELATIONAL );

        long tableId = catalog.addTable( "table1", schemaId, userId, TableType.TABLE, true );

        List<String> columnNames = new ArrayList<>( Arrays.asList( "column1", "column2", "column3", "column4", "column5" ) );
        List<Long> columnIds = new ArrayList<>();
        int counter = 0;
        for ( String name : columnNames ) {
            columnIds.add( catalog.addColumn( name, tableId, counter++, PolyType.BIGINT, null, null, null, null, null, false, null ) );
        }

        // test rename of column
        long columnId = columnIds.get( 0 );
        String newColumnName = "newColumn";
        catalog.renameColumn( columnId, newColumnName );

        columnNames.remove( 0 );
        columnNames.add( 0, newColumnName );

        assertEquals( catalog.getColumns( tableId ).stream().map( c -> c.name ).collect( Collectors.toList() ), columnNames );

        // test replacing ColumnType
        catalog.setColumnType( columnId, PolyType.CHAR, null, null, null, null, null );

        assertEquals( PolyType.CHAR, catalog.getColumn( columnId ).type );

        // test replacing collation
        catalog.setCollation( columnId, Collation.CASE_INSENSITIVE );

        assertEquals( Collation.CASE_INSENSITIVE, catalog.getColumn( columnId ).collation );

        // test replacing position
        long otherColumnId = columnIds.get( 1 );
        CatalogColumn column = catalog.getColumn( columnId );
        CatalogColumn otherColumn = catalog.getColumn( otherColumnId );

        catalog.setColumnPosition( columnId, otherColumn.position );
        catalog.setColumnPosition( otherColumnId, column.position );

        assertEquals( otherColumn.position, catalog.getColumn( columnId ).position );
        assertEquals( column.position, catalog.getColumn( otherColumnId ).position );

        // test replacing default value
        catalog.setDefaultValue( columnId, PolyType.CHAR, "i" );
        assertEquals( "i", catalog.getColumn( columnId ).defaultValue.value );

        // test setting nullable
        catalog.setNullable( columnId, true );
        assertTrue( catalog.getColumn( columnId ).nullable );
        catalog.setNullable( columnId, false );
        assertFalse( catalog.getColumn( columnId ).nullable );

        for ( long id : columnIds ) {
            catalog.deleteColumn( id );
        }
        assertTrue( catalog.getColumns( tableId ).isEmpty() );

    }


    public void addStores() {
        Map<String, String> hsqldbSettings = new HashMap<>();
        hsqldbSettings.put( "type", "Memory" );
        hsqldbSettings.put( "path", "maxConnections" );
        hsqldbSettings.put( "maxConnections", "25" );
        hsqldbSettings.put( "trxControlMode", "mvcc" );
        hsqldbSettings.put( "trxIsolationLevel", "read_committed" );

        catalog.addAdapter( "store1", "org.polypheny.db.adapter.jdbc.stores.HsqldbStore", AdapterType.STORE, hsqldbSettings );
        catalog.addAdapter( "store2", "org.polypheny.db.adapter.jdbc.stores.HsqldbStore", AdapterType.STORE, hsqldbSettings );
    }


    @Test
    public void testColumnPlacement() throws UnknownAdapterException {
        addStores();

        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );
        long schemaId = catalog.addSchema( "schema1", databaseId, userId, SchemaType.RELATIONAL );
        long tableId = catalog.addTable( "table1", schemaId, userId, TableType.TABLE, true );

        long columnId = catalog.addColumn( "column1", tableId, 0, PolyType.BIGINT, null, null, null, null, null, false, null );
        CatalogColumn column = catalog.getColumn( columnId );

        CatalogAdapter store1 = catalog.getAdapter( "store1" );
        CatalogAdapter store2 = catalog.getAdapter( "store2" );

        catalog.addColumnPlacement( store1.id, columnId, PlacementType.AUTOMATIC, null, "table1", column.name );

        assertEquals( 1, catalog.getColumnPlacement( columnId ).size() );
        assertEquals( columnId, catalog.getColumnPlacement( columnId ).get( 0 ).columnId );

        catalog.addColumnPlacement( store2.id, columnId, PlacementType.AUTOMATIC, null, "table1", column.name );

        assertEquals( 2, catalog.getColumnPlacement( columnId ).size() );
        assertTrue( catalog.getColumnPlacement( columnId ).stream().map( p -> p.adapterId ).collect( Collectors.toList() ).containsAll( Arrays.asList( store2.id, store1.id ) ) );

        catalog.deleteColumnPlacement( store1.id, columnId, false );
        assertEquals( 1, catalog.getColumnPlacement( columnId ).size() );
        assertEquals( store2.id, catalog.getColumnPlacement( columnId ).get( 0 ).adapterId );
    }


    @Test
    public void testKey() throws GenericCatalogException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );
        long schemaId = catalog.addSchema( "schema1", databaseId, userId, SchemaType.RELATIONAL );
        long tableId = catalog.addTable( "table1", schemaId, userId, TableType.TABLE, true );

        long columnId1 = catalog.addColumn( "column1", tableId, 0, PolyType.BIGINT, null, null, null, null, null, false, null );
        CatalogColumn column1 = catalog.getColumn( columnId1 );

        long columnId2 = catalog.addColumn( "column2", tableId, 0, PolyType.BIGINT, null, null, null, null, null, false, null );
        CatalogColumn column2 = catalog.getColumn( columnId2 );

        catalog.addPrimaryKey( tableId, Collections.singletonList( column1.id ) );

        assertEquals( 1, catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey ).columnIds.size() );
        assertTrue( catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey ).columnIds.contains( columnId1 ) );

        //catalog.deletePrimaryKey( tableId );
        //assertNull( catalog.getTable( tableId ).primaryKey );

        catalog.addPrimaryKey( tableId, Arrays.asList( columnId1, columnId2 ) );

        assertEquals( 2, catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey ).columnIds.size() );
        assertTrue( catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey ).columnIds.contains( columnId1 ) );
        assertTrue( catalog.getPrimaryKey( catalog.getTable( tableId ).primaryKey ).columnIds.contains( columnId2 ) );

        //catalog.deletePrimaryKey( tableId );

        // test constraints
        String constraint1 = "unique constraint";
        catalog.addUniqueConstraint( tableId, constraint1, Collections.singletonList( columnId1 ) );

        String constraint2 = "other constraint";
        catalog.addUniqueConstraint( tableId, constraint2, Arrays.asList( columnId1, columnId2 ) );

        assertEquals( 2, catalog.getConstraints( tableId ).size() );
        assertTrue( catalog.getConstraints( tableId ).stream().map( c -> c.name ).collect( Collectors.toList() ).containsAll( Arrays.asList( constraint1, constraint2 ) ) );

        if ( catalog.getConstraints( tableId ).get( 0 ).key.columnIds.size() == 1 ) {
            assertTrue( catalog.getConstraints( tableId ).get( 0 ).key.columnIds.contains( columnId1 ) );
            assertTrue( catalog.getConstraints( tableId ).get( 1 ).key.columnIds.containsAll( Arrays.asList( column1.id, column2.id ) ) );

            catalog.deleteConstraint( catalog.getConstraints( tableId ).get( 0 ).id );

            assertEquals( 1, catalog.getConstraints( tableId ).size() );
            assertTrue( catalog.getConstraints( tableId ).get( 0 ).key.columnIds.containsAll( Arrays.asList( column1.id, column2.id ) ) );

            catalog.deleteConstraint( catalog.getConstraints( tableId ).get( 0 ).id );

        } else {
            assertTrue( catalog.getConstraints( tableId ).get( 1 ).key.columnIds.contains( columnId1 ) );
            assertTrue( catalog.getConstraints( tableId ).get( 0 ).key.columnIds.containsAll( Arrays.asList( column1.id, column2.id ) ) );

            catalog.deleteConstraint( catalog.getConstraints( tableId ).get( 0 ).id );

            assertEquals( 1, catalog.getConstraints( tableId ).size() );
            assertTrue( catalog.getConstraints( tableId ).get( 0 ).key.columnIds.contains( column1.id ) );

            catalog.deleteConstraint( catalog.getConstraints( tableId ).get( 0 ).id );
        }

        // test foreign key
        long tableId2 = catalog.addTable( "table2", schemaId, userId, TableType.TABLE, true );
        long columnId3 = catalog.addColumn( "column3", tableId2, 0, PolyType.BIGINT, null, null, null, null, null, false, null );
        CatalogColumn column3 = catalog.getColumn( columnId3 );

        catalog.addPrimaryKey( tableId, Collections.singletonList( columnId1 ) );
        catalog.addForeignKey( tableId2, Collections.singletonList( columnId3 ), tableId, Collections.singletonList( columnId1 ), "name", ForeignKeyOption.RESTRICT, ForeignKeyOption.RESTRICT );
        assertEquals( 1, catalog.getForeignKeys( tableId2 ).size() );
        assertEquals( 1, catalog.getForeignKeys( tableId2 ).get( 0 ).columnIds.size() );
        assertEquals( columnId3, (long) catalog.getForeignKeys( tableId2 ).get( 0 ).columnIds.get( 0 ) );
        assertEquals( columnId1, (long) catalog.getForeignKeys( tableId2 ).get( 0 ).referencedKeyColumnIds.get( 0 ) );

        catalog.deleteForeignKey( catalog.getForeignKeys( tableId2 ).get( 0 ).id );
        //catalog.deletePrimaryKey( tableId );

        assertEquals( 0, catalog.getForeignKeys( tableId ).size() );

        // test index
        assertEquals( 0, catalog.getIndexes( tableId, false ).size() );
        catalog.addIndex( tableId, Collections.singletonList( columnId1 ), false, "btree", "BTREE", 1, IndexType.MANUAL, "index" );
        assertEquals( 1, catalog.getIndexes( tableId, false ).size() );
        assertEquals( 0, catalog.getIndexes( tableId, true ).size() );
        catalog.deleteIndex( catalog.getIndexes( tableId, false ).get( 0 ).id );
        assertEquals( 0, catalog.getIndexes( tableId, false ).size() );

    }


    @Test
    public void performanceTests() {
        int iterations = 1000;
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for ( int i = 0; i < iterations; i++ ) {
            long id = catalog.addDatabase( "APP", userId, user.name, 0, "" );
            catalog.deleteDatabase( id );
        }
        stopWatch.stop();
        log.warn( "{}ms iterations needed, means 1 needed {}ms", stopWatch.getTime(), stopWatch.getTime() / 1000 );
    }


    @After
    public void close() {
        catalog.close();
    }

}
