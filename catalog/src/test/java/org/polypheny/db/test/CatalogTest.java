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
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
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
        catalog.setSchemaOwner( 0, newUserId );

        assertEquals( catalog.getSchema( 0, replacedName ).ownerId, newUserId );
    }

    @Test
    public void testTable() {

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
