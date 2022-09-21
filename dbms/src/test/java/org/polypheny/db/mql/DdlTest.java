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

package org.polypheny.db.mql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.webui.models.Result;

@SuppressWarnings("SqlNoDataSourceInspection")
@Category({ AdapterTestSuite.class })
public class DdlTest extends MqlTestTemplate {

    final static String collectionName = "doc";


    @After
    public void removeCollection() {
        execute( String.format( "db.%s.drop()", collectionName ) );
    }


    @Test
    public void addCollectionTest() throws UnknownSchemaException {
        Catalog catalog = Catalog.getInstance();

        CatalogSchema namespace = catalog.getSchema( Catalog.defaultDatabaseId, database );

        int size = catalog.getCollections( namespace.id, null ).size();

        execute( "db.createCollection(\"" + collectionName + "\")" );

        assertEquals( size + 1, catalog.getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", collectionName ) );

        assertEquals( size, catalog.getCollections( namespace.id, null ).size() );

        execute( "db.createCollection(\"" + collectionName + "\")" );

        assertEquals( size + 1, catalog.getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", collectionName ) );
    }


    @Test
    public void addPlacementTest() throws UnknownSchemaException, SQLException {
        Catalog catalog = Catalog.getInstance();

        String placement = "store1";
        try {
            CatalogSchema namespace = catalog.getSchema( Catalog.defaultDatabaseId, database );

            List<String> collectionNames = catalog.getCollections( namespace.id, null ).stream().map( c -> c.name ).collect( Collectors.toList() );
            collectionNames.forEach( n -> execute( String.format( "db.%s.drop()", n ) ) );

            execute( "db.createCollection(\"" + collectionName + "\")" );

            CatalogCollection collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( collection.placements.size(), 1 );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( collection.placements.size(), 2 );

        } finally {
            execute( String.format( "db.%s.drop()", collectionName ) );
            removeStore( placement );
        }

    }


    @Test
    public void deletePlacementTest() throws UnknownSchemaException, SQLException {
        Catalog catalog = Catalog.getInstance();

        String placement = "store1";
        try {

            execute( "db.createCollection(\"" + collectionName + "\")" );

            CatalogSchema namespace = catalog.getSchema( Catalog.defaultDatabaseId, database );

            CatalogCollection collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( collection.placements.size(), 1 );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( collection.placements.size(), 2 );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, placement ) );

            collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( collection.placements.size(), 1 );

            execute( String.format( "db.%s.drop()", collectionName ) );

        } finally {
            removeStore( placement );
        }
    }


    @Test
    @Category(CassandraExcluded.class)
    public void deletePlacementDataTest() throws SQLException {

        String placement = "store1";
        final String DATA = "{ \"key\": \"value\", \"key1\": \"value1\"}";
        try {

            execute( "db.createCollection(\"" + collectionName + "\")" );

            insert( DATA );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, "hsqldb" ) );

            Result result = find( "{}", "{}" );

            assertTrue(
                    MongoConnection.checkResultSet(
                            result,
                            ImmutableList.of( new Object[]{ DATA } ), true ) );

            execute( String.format( "db.%s.drop()", collectionName ) );

        } finally {
            removeStore( placement );
        }
    }


    private void addStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "ALTER ADAPTERS ADD \"" + name + "\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

            }
        }
    }


    private void removeStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "ALTER ADAPTERS DROP \"" + name + "\"" );

            }
        }
    }

}
