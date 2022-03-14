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

package org.polypheny.db.cypher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Locale;
import javax.annotation.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestObject;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.webui.models.Result;

public class CypherTestTemplate {


    public static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();


    @BeforeClass
    public static void start() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        createSchema();
    }


    public static void createSchema() {
        execute( "CREATE DATABASE test" );
        execute( "USE GRAPH test" );
    }


    public static void createData() {

    }


    @AfterClass
    public static void tearDown() {
        deleteData();
    }


    private static void deleteData() {
        execute( "DROP DATABASE test IF EXISTS" );
    }


    public static Result execute( String query ) {
        return CypherConnection.executeGetResponse( query );
    }


    public static String matches( String child, String... matches ) {
        return concatStatement( "MATCH", child, matches );
    }


    public static String returns( String child, String... returns ) {
        return concatStatement( "RETURN", child, returns );
    }


    public static String with( String child, String... withs ) {
        return concatStatement( "WITH", child, withs );
    }


    public static String where( String child, String... conditions ) {
        return concatStatement( "WHERE", child, conditions );
    }


    public static String concatStatement( String keyword, @Nullable String child, String... variables ) {
        String line = String.format( "%s %s", keyword, String.join( ",", variables ) );
        if ( child != null ) {
            line += String.format( "\n%s", child );
        }
        return line;
    }


    protected boolean containsNodes( Result res, boolean exclusive, TestObject... nodes ) {
        if ( res.getHeader().length == 1 && res.getHeader()[0].dataType.toLowerCase( Locale.ROOT ).contains( "graph" ) ) {
            return containsNodesInGraph( res.getData()[0][0], exclusive, nodes );
        }
        throw new UnsupportedOperationException();
    }


    protected boolean containsEdges( Result res, boolean exclusive, TestEdge... edges ) {
        if ( res.getHeader().length == 1 && res.getHeader()[0].dataType.toLowerCase( Locale.ROOT ).contains( "graph" ) ) {
            return containsEdgesInGraph( res.getData()[0][0], exclusive, edges );
        }
        throw new UnsupportedOperationException();
    }


    private boolean containsEdgesInGraph( String graph, boolean exclusive, TestEdge[] edges ) {
        PolyGraph parsed = GSON.fromJson( graph, PolyGraph.class );
        assert !exclusive || parsed.getEdges().size() == edges.length;

        boolean contains = true;
        for ( TestEdge edge : edges ) {
            contains &= parsed.getEdges().values().stream().anyMatch( e -> edge.matches( e, exclusive ) );
        }

        return contains;
    }


    private boolean containsNodesInGraph( String graph, boolean exclusive, TestObject[] nodes ) {
        PolyGraph parsed = GSON.fromJson( graph, PolyGraph.class );
        assert !exclusive || parsed.getNodes().size() == nodes.length;

        boolean contains = true;
        for ( TestObject node : nodes ) {
            contains &= parsed.getNodes().values().stream().anyMatch( n -> node.matches( n, exclusive ) );
        }

        return contains;
    }


    public boolean emptyNodes( Result res ) {
        return containsNodes( res, true );
    }


    public boolean emptyEdges( Result res ) {
        return containsEdges( res, true );
    }


    public Result matchAndReturnN() {
        return execute( "MATCH (n) RETURN n" );
    }

}
