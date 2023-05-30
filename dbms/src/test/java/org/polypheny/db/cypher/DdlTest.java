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

package org.polypheny.db.cypher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.webui.models.Result;

@Category({ AdapterTestSuite.class })
public class DdlTest extends CypherTestTemplate {

    final static String graphName = "product";


    @Test
    public void addGraphTest() {

        execute( "CREATE DATABASE " + graphName );

        assertNotNull( Catalog.snapshot().getNamespace( graphName ) );

        execute( "DROP DATABASE " + graphName );

        assertNull( Catalog.snapshot().getNamespace( graphName ) );

        execute( "CREATE DATABASE " + graphName );

        assertNotNull( Catalog.snapshot().getNamespace( graphName ) );

        execute( "DROP DATABASE " + graphName );
    }


    @Test
    public void addPlacementTest() throws SQLException {
        Catalog catalog = Catalog.getInstance();
        try {
            execute( "CREATE DATABASE " + graphName );

            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName );
            LogicalGraph graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            addStore( "store1" );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            namespace = catalog.getSnapshot().getNamespace( graphName );
            graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 2, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( "DROP DATABASE " + graphName );

        } finally {

            removeStore( "store1" );
        }

    }


    @Test
    public void initialPlacementTest() throws SQLException {
        Catalog catalog = Catalog.getInstance();
        try {
            addStore( "store1" );

            execute( String.format( "CREATE DATABASE %s ON STORE %s", graphName, "store1" ) );
            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName );
            LogicalGraph graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "hsqldb" ), graphName );

            namespace = catalog.getSnapshot().getNamespace( graphName );
            graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );
            assertEquals( 2, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( "DROP DATABASE " + graphName );

        } finally {
            removeStore( "store1" );
        }

    }


    @Test
    @Category(CassandraExcluded.class)
    public void deletePlacementTest() throws SQLException {
        try {
            Catalog catalog = Catalog.getInstance();

            execute( "CREATE DATABASE " + graphName );

            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName );
            LogicalGraph graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            addStore( "store1" );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            namespace = catalog.getSnapshot().getNamespace( graphName );
            graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 2, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( String.format( "DROP PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            execute( "DROP DATABASE " + graphName );

        } finally {
            removeStore( "store1" );
        }

    }


    @Test
    @Category(CassandraExcluded.class)
    public void deletePlacementDataTest() throws SQLException {

        execute( "CREATE DATABASE " + graphName );

        execute( DmlInsertTest.CREATE_COMPLEX_GRAPH_2 );

        try {
            addStore( "store1" );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            execute( String.format( "DROP PLACEMENT OF %s ON STORE %s", graphName, "hsqldb" ), graphName );

            Result res = execute( "MATCH (n) RETURN n" );
            assert res.getData().length == 3;
            assertNode( res, 0 );

            res = execute( "MATCH ()-[r]->() RETURN r" );
            assert res.getData().length == 3;
            assertEdge( res, 0 );

            execute( "DROP DATABASE " + graphName );

        } finally {
            removeStore( "store1" );
        }

    }


    private void addStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( String.format( "ALTER ADAPTERS ADD \"%s\" USING 'Hsqldb' AS 'Store'"
                        + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'", name ) );

            }
        }
    }


    private void removeStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( String.format( "ALTER ADAPTERS DROP \"%s\"", name ) );

            }
        }
    }

}
