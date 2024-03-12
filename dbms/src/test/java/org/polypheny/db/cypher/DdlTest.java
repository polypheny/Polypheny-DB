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

package org.polypheny.db.cypher;


import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.webui.models.results.GraphResult;

@Tag("adapter")
@Slf4j
public class DdlTest extends CypherTestTemplate {

    final static String graphName = "product";


    @Test
    public void addGraphTest() {
        execute( "CREATE DATABASE " + graphName + " IF NOT EXISTS" );

        assertTrue( Catalog.snapshot().getNamespace( graphName ).isPresent() );

        execute( "DROP DATABASE " + graphName );

        assertTrue( Catalog.snapshot().getNamespace( graphName ).isEmpty() );

        execute( "CREATE DATABASE " + graphName );

        assertTrue( Catalog.snapshot().getNamespace( graphName ).isPresent() );

        execute( "DROP DATABASE " + graphName );

    }


    @ParameterizedTest(name = "Create namespace with naming: {0}")
    @ValueSource(strings = { "DATABASE", "NAMESPACE" })
    public void createNamespaceTest( String namespaceName ) {
        String name = "namespaceTest";

        execute( format( "CREATE %s %s", namespaceName, name ) );

        execute( format( "DROP %s %s", namespaceName, name ) );

    }

    @Test
    public void addPlacementTest() throws SQLException {
        Catalog catalog = Catalog.getInstance();
        try {
            execute( "CREATE DATABASE " + graphName + " IF NOT EXISTS" );

            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
            LogicalGraph graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            addStore( "store1" );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
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

            execute( String.format( "CREATE DATABASE %s IF NOT EXISTS ON STORE %s ", graphName, "store1" ) );
            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
            LogicalGraph graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "hsqldb" ), graphName );

            namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
            graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 2, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( "DROP DATABASE " + graphName );

        } finally {
            removeStore( "store1" );
        }

    }


    @Test
    public void deletePlacementTest() throws SQLException {
        try {
            Catalog catalog = Catalog.getInstance();

            execute( "CREATE DATABASE " + graphName + " IF NOT EXISTS" );

            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
            LogicalGraph graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 1, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            addStore( "store1" );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
            graph = catalog.getSnapshot().graph().getGraph( namespace.id ).orElseThrow();

            assertEquals( 2, catalog.getSnapshot().alloc().getFromLogical( graph.id ).size() );

            execute( String.format( "DROP PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            execute( "DROP DATABASE " + graphName );

        } finally {
            removeStore( "store1" );
        }

    }


    @Test
    public void deletePlacementDataTest() throws SQLException {

        execute( "CREATE DATABASE " + graphName + " IF NOT EXISTS" );

        execute( DmlInsertTest.CREATE_COMPLEX_GRAPH_2, graphName );

        try {
            addStore( "store1" );

            execute( String.format( "CREATE PLACEMENT OF %s ON STORE %s", graphName, "store1" ), graphName );

            execute( String.format( "DROP PLACEMENT OF %s ON STORE %s", graphName, "hsqldb" ), graphName );

            GraphResult res = execute( "MATCH (n) RETURN n", graphName );
            assert res.getData().length == 3;
            assertNode( res, 0 );

            res = execute( "MATCH ()-[r]->() RETURN r", graphName );
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
                TestHelper.addHsqldb( name, statement );
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
