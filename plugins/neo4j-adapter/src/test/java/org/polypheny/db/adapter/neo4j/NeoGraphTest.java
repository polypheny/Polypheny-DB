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

package org.polypheny.db.adapter.neo4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.webui.models.results.GraphResult;
import org.polypheny.db.util.Pair;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.fail;

public class NeoGraphTest extends CypherTestTemplate {
    final static String GRAPH_NAMESPACE = "neotest";
    final static String GRAPH_STORE = "neo4j1";
    @BeforeAll
    public static void start() {
        try {
            TestHelper.executeSQL( String.format( "ALTER ADAPTERS ADD \"%s\" USING 'Neo4j' AS 'STORE' WITH '{\"mode\":\"DOCKER\",\"instanceId\":\"0\"}'" , GRAPH_STORE ) );
            CypherConnection.executeGetResponse( String.format( "CREATE DATABASE %s ON STORE %s", GRAPH_NAMESPACE, GRAPH_STORE ) );
        } catch ( Exception e ) {
            fail("Error on setting a neo4j docker store " + e.getMessage() );
        }
    }

    @AfterAll
    public static void tearDown() {
        try {
            CypherConnection.executeGetResponse( String.format( "DROP DATABASE %s", GRAPH_NAMESPACE ) );
            TestHelper.executeSQL( String.format( "ALTER ADAPTERS DROP %s", GRAPH_STORE ) );
        } catch ( SQLException e ) {
            fail( "Error on destroying neo4j store" + e.getMessage() );
        }
    }

    @Test
    public void insertNodeTest() {
        CypherConnection.executeGetResponse( "CREATE (p:Person {name: \"Max Muster\"})",GRAPH_NAMESPACE );
        GraphResult res = CypherConnection.executeGetResponse( "MATCH (n) RETURN n", GRAPH_NAMESPACE );
        assertNode( res, 0 );
        assert containsNodes( res, false, TestNode.from( Pair.of( "name", "Max Muster" ) ) );
    }

    @Test
    public void insertNodeWithSingleQouteTest() {
        final String query = "CREATE (n:Message {message: \"I'd like some water\"})";
        CypherConnection.executeGetResponse( query, GRAPH_NAMESPACE );
        GraphResult res = CypherConnection.executeGetResponse( "MATCH (n) RETURN n", GRAPH_NAMESPACE );
        assertNode( res, 0 );
        assert containsNodes( res, false, TestNode.from( Pair.of( "message", "I'd like some water" ) ) );
    }

    @Test
    public void insertStringWithDoubleQuotes() {
        final String query = "CREATE (n:Message {message: 'Hello, \"world\"'})";
        CypherConnection.executeGetResponse( query, GRAPH_NAMESPACE );
        GraphResult res = CypherConnection.executeGetResponse( "MATCH (n) RETURN n", GRAPH_NAMESPACE );
        assertNode( res, 0 );
        assert containsNodes( res, false, TestNode.from( Pair.of( "message", "Hello, \"world\"" ) ) );
    }

    @Test
    public void insertStringWithSingleQuoteAndDoubleQuotes() {
        final String query = "CREATE (n:Message {message: \"I'd like some \\\"tea\\\"\"})";
        CypherConnection.executeGetResponse( query, GRAPH_NAMESPACE );
        GraphResult res = CypherConnection.executeGetResponse( "MATCH (n) RETURN n", GRAPH_NAMESPACE );
        assertNode( res, 0 );
        assert containsNodes( res, false, TestNode.from( Pair.of( "message", "I'd like some \"tea\"" ) ) );
    }
}
