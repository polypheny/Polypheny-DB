/*
 * Copyright 2019-2024 The Polypheny Project
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.webui.models.results.DocResult;

@SuppressWarnings("SqlNoDataSourceInspection")
@Tag("adapter")
public class DdlTest extends MqlTestTemplate {

    final static String collectionName = "doc";


    @Test
    public void addCollectionTest() {
        String name = "testCollection";

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( MqlTestTemplate.namespace ).orElseThrow();

        int size = Catalog.snapshot().doc().getCollections( namespace.id, null ).size();

        execute( "db.createCollection(\"" + name + "\")" );

        assertEquals( size + 1, Catalog.snapshot().doc().getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", name ) );

        assertEquals( size, Catalog.snapshot().doc().getCollections( namespace.id, null ).size() );

        execute( "db.createCollection(\"" + name + "\")" );

        assertEquals( size + 1, Catalog.snapshot().doc().getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", name ) );
    }


    @Test
    public void differentNamespaceSyntaxTest() {
        String name = "testNamespaceSyntax";

        execute( namespace + ".createCollection(\"" + name + "\")" );

        execute( "db." + name + ".find({})" );

        execute( name + ".find({})" );

        execute( namespace + "." + name + ".find({})" );

        execute( String.format( "%s.%s.drop()", namespace, name ) );

    }


    @Test
    public void addPlacementTest() throws SQLException {

        String placement = "store1";
        try {
            LogicalNamespace namespace = Catalog.snapshot().getNamespace( MqlTestTemplate.namespace ).orElseThrow();

            List<String> collectionNames = Catalog.snapshot().doc().getCollections( namespace.id, null ).stream().map( c -> c.name ).toList();
            collectionNames.forEach( n -> execute( String.format( "db.%s.drop()", n ) ) );

            execute( "db.createCollection(\"" + collectionName + "\")" );

            LogicalCollection collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 1 );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 2 );

        } finally {
            execute( String.format( "db.%s.drop()", collectionName ) );
            removeStore( placement );
        }

    }


    @Test
    public void deletePlacementTest() throws SQLException {

        String placement = "store1";

        execute( "db.createCollection(\"" + collectionName + "\")" );

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( MqlTestTemplate.namespace ).orElseThrow();

        LogicalCollection collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

        assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 1 );

        addStore( placement );

        try {
            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 2 );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, placement ) );

            collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 1 );

            execute( String.format( "db.%s.drop()", collectionName ) );
        } catch ( Exception e ) {
            execute( String.format( "db.%s.drop()", collectionName ) );
        } finally {
            removeStore( placement );
        }
    }


    @Test
    public void deletePlacementDataTest() throws SQLException {

        String placement = "store1";
        final String DATA = "{ \"key\": \"value\", \"key1\": \"value1\"}";

        execute( "db.createCollection(\"" + collectionName + "\")" );

        insert( DATA );

        try {

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, "hsqldb" ) );

            DocResult result = find( "{}", "{}" );

            assertTrue(
                    MongoConnection.checkDocResultSet(
                            result,
                            ImmutableList.of( DATA ), true,
                            false ) );


        } finally {
            execute( String.format( "db.%s.drop()", collectionName ) );

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
