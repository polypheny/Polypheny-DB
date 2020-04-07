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

package org.polypheny.db.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.CatalogImpl;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.type.PolyType;


public class CatalogTest {

    CatalogImpl catalog;


    @Before
    public void setup() {
        catalog = new CatalogImpl( "testDB", false, false );
        catalog.clear();
    }


    @After
    public void cleanup() {
        catalog.closeAndDelete();
    }


    @Test
    public void testInit() {
        assert (catalog != null);
    }


    @Test
    public void testLayout() throws UnknownUserException, UnknownDatabaseException, GenericCatalogException, UnknownSchemaException, UnknownTableException, UnknownColumnException {
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

        long tableId = catalog.addTable( "test_table", schemaId, userId, TableType.TABLE, null );
        CatalogTable table = catalog.getTable( tableId, "test_table" );
        assertEquals( tableId, table.id );

        long columnId = catalog.addColumn( "test_column", tableId, 0, PolyType.BIGINT, null, null, false, null );
        CatalogColumn column = catalog.getColumn( tableId, "test_column" );
        assertEquals( columnId, column.id );
    }


    @Test
    public void testDatabase() throws UnknownUserException, UnknownDatabaseException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        List<String> names = Arrays.asList( "database1", "database2", "database3" );
        List<Long> ids = new ArrayList<>();
        for ( String name : names ) {
            ids.add( catalog.addDatabase( name, userId, user.name, 0, "" ) );
        }

        assertEquals( catalog.getDatabases( null ).stream().map( d -> d.name ).collect( Collectors.toList() ), names );

        for ( Long id : ids ) {
            catalog.removeDatabase( id );
        }

        assertEquals( catalog.getDatabases( null ), Collections.emptyList() );

    }


    @Test
    public void testSchema() throws UnknownUserException, UnknownDatabaseException, GenericCatalogException, UnknownSchemaException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );
        CatalogDatabase database = catalog.getDatabase( databaseId );

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
    public void testTable() throws UnknownUserException, UnknownDatabaseException, GenericCatalogException, UnknownTableException, UnknownKeyException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );

        long databaseId = catalog.addDatabase( "APP", userId, user.name, 0, "" );
        CatalogDatabase database = catalog.getDatabase( databaseId );

        long schemaId = catalog.addSchema( "schema1", databaseId, userId, SchemaType.RELATIONAL );
        CatalogSchema schema = catalog.getSchema( schemaId );

        List<String> names = new ArrayList<>( Arrays.asList( "table1", "table2", "table3", "table4", "table5" ) );
        List<Long> ids = new ArrayList<>();

        for ( String name : names ) {
            ids.add( catalog.addTable( name, schemaId, userId, TableType.TABLE, null ) );
        }

        // test renaming table
        String newTable = "newTable";
        Long id = ids.get( 3 );

        names.remove( 3 );
        names.add( 3, newTable );

        catalog.renameTable( id, newTable );
        assertEquals( catalog.getTables( null, null, null ).stream().sorted().map( s -> s.name ).collect( Collectors.toList() ), names );

        // test change owner
        String newUserName = "newUser";
        int newUserId = catalog.addUser( newUserName, "" );
        catalog.setTableOwner( id, newUserId );

        assertEquals( catalog.getTable( id ).ownerId, newUserId );
        assertEquals( catalog.getTable( id ).ownerName, newUserName );

        // test change primary
        List<String> columnNames = new ArrayList<>( Arrays.asList( "column1", "column2" ) );
        List<Long> columnIds = new ArrayList<>();
        int counter = 0;
        for ( String name : columnNames ) {
            columnIds.add( catalog.addColumn( name, id, counter++, PolyType.BIGINT, null, null, false, null ) );
        }

        Long columnId = columnIds.get( 0 );
        catalog.addPrimaryKey( id, new ArrayList<>( Collections.singleton( columnId ) ) );

        CatalogPrimaryKey key = catalog.getPrimaryKey( catalog.getTable( id ).primaryKey );
        assertEquals( key.columnIds.get( 0 ), columnId );

        catalog.deletePrimaryKey( id );
        assertNull( catalog.getTable( id ).primaryKey );

        catalog.addPrimaryKey( id, columnIds );
        key = catalog.getPrimaryKey( catalog.getTable( id ).primaryKey );
        assertEquals(  key.columnIds, columnIds);


    }


    @Test
    public void testColumn() {

    }


    @Test
    public void testColumnPlacement() {

    }


    @After
    public void close() {
        catalog.close();
    }


}
