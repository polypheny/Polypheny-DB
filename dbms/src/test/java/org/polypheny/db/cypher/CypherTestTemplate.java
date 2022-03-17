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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestObject;
import org.polypheny.db.schema.graph.GraphPropertyHolder;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyNode;
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
        if ( res.getHeader().length == 1 && res.getHeader()[0].dataType.toLowerCase( Locale.ROOT ).contains( "node" ) ) {
            return contains( res.getData(), exclusive, 0, PolyNode.class, nodes );
        }
        throw new UnsupportedOperationException();
    }


    protected boolean containsEdges( Result res, boolean exclusive, TestEdge... edges ) {
        if ( res.getHeader().length == 1 && res.getHeader()[0].dataType.toLowerCase( Locale.ROOT ).contains( "edge" ) ) {
            return contains( res.getData(), exclusive, 0, PolyEdge.class, edges );
        }
        throw new UnsupportedOperationException();
    }


    private <T extends GraphPropertyHolder> boolean contains( String[][] actual, boolean exclusive, int index, Class<T> clazz, TestObject[] expected ) {
        List<T> parsed = new ArrayList<>();

        for ( String[] entry : actual ) {
            parsed.add( GSON.fromJson( entry[index], clazz ) );
        }

        assert !exclusive || parsed.size() == expected.length;

        boolean contains = true;
        for ( TestObject node : expected ) {
            contains &= parsed.stream().anyMatch( n -> node.matches( n, exclusive ) );
        }

        return contains;
    }


    public boolean emptyNodes( Result res ) {
        return containsNodes( res, true );
    }


    public boolean emptyEdges( Result res ) {
        return containsEdges( res, true );
    }


    public Result matchAndReturnAllNodes() {
        return execute( "MATCH (n) RETURN n" );
    }


    protected boolean isNode( Result res ) {
        return is( res, "node", 0 );
    }


    protected boolean isEdge( Result res ) {
        return is( res, "edge", 0 );
    }


    protected boolean is( Result res, String type, int index ) {
        assert res.getHeader().length >= index;

        return res.getHeader()[index].dataType.toLowerCase( Locale.ROOT ).contains( type );
    }


    protected boolean isEmpty( Result res ) {
        return res.getData().length == 0;
    }

}
