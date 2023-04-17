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
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.webui.models.Result;

@SuppressWarnings("SqlNoDataSourceInspection")
@Category({ AdapterTestSuite.class, CassandraExcluded.class }) // cassandra can only compare primary key equality, but for streamer each key has to be compared
public class DdlTest extends MqlTestTemplate {

    final static String collectionName = "doc";


    @After
    public void removeCollection() {
        execute( String.format( "db.%s.drop()", collectionName ) );
    }


    @Test
    public void addCollectionTest() {
        Snapshot snapshot = Catalog.snapshot();
        String name = "testCollection";

        LogicalNamespace namespace = snapshot.getNamespace( database );

        int size = snapshot.doc().getCollections( namespace.id, null ).size();

        execute( "db.createCollection(\"" + name + "\")" );

        assertEquals( size + 1, snapshot.doc().getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", name ) );

        assertEquals( size, snapshot.doc().getCollections( namespace.id, null ).size() );

        execute( "db.createCollection(\"" + name + "\")" );

        assertEquals( size + 1, snapshot.doc().getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", name ) );
    }


    @Test
    public void addPlacementTest() throws SQLException {
        Snapshot snapshot = Catalog.snapshot();

        String placement = "store1";
        try {
            LogicalNamespace namespace = snapshot.getNamespace( database );

            List<String> collectionNames = snapshot.doc().getCollections( namespace.id, null ).stream().map( c -> c.name ).collect( Collectors.toList() );
            collectionNames.forEach( n -> execute( String.format( "db.%s.drop()", n ) ) );

            execute( "db.createCollection(\"" + collectionName + "\")" );

            LogicalCollection collection = snapshot.doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( snapshot.alloc().getDataPlacements( collection.id ).size(), 1 );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = snapshot.doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getDataPlacements( collection.id ).size(), 2 );

        } finally {
            execute( String.format( "db.%s.drop()", collectionName ) );
            removeStore( placement );
        }

    }


    @Test
    public void deletePlacementTest() throws SQLException {
        Snapshot snapshot = Catalog.snapshot();

        String placement = "store1";
        try {

            execute( "db.createCollection(\"" + collectionName + "\")" );

            LogicalNamespace namespace = snapshot.getNamespace( database );

            LogicalCollection collection = snapshot.doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getDataPlacements( collection.id ).size(), 1 );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = snapshot.doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getDataPlacements( collection.id ).size(), 2 );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, placement ) );

            collection = snapshot.doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getDataPlacements( collection.id ).size(), 1 );

            execute( String.format( "db.%s.drop()", collectionName ) );

        } finally {
            removeStore( placement );
        }
    }


    @Test
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

                statement.executeUpdate( "ALTER ADAPTERS ADD \"" + name + "\" USING 'Hsqldb' AS 'Store'"
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
