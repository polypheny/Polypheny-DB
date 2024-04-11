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
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestGraphObject;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.cypher.helper.TestObject;
import org.polypheny.db.cypher.helper.TestPath;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

@Slf4j
public class CypherTestTemplate {

    private static final String GRAPH_NAME = "test";
    protected static final String SINGLE_NODE_PERSON_1 = "CREATE (p:Person {name: 'Max'})";
    protected static final String SINGLE_NODE_PERSON_2 = "CREATE (p:Person {name: 'Hans'})";

    protected static final String SINGLE_NODE_PERSON_COMPLEX_1 = "CREATE (p:Person {name: 'Ann', age: 45, depno: 13})";
    protected static final String SINGLE_NODE_PERSON_COMPLEX_2 = "CREATE (p:Person {name: 'Bob', age: 31, depno: 13})";

    protected static final String SINGLE_NODE_ANIMAL = "CREATE (a:Animal {name:'Kira', age:3, type:'dog'})";
    protected static final String SINGLE_EDGE_1 = "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})";
    protected static final String SINGLE_EDGE_2 = "CREATE (p:Person {name: 'Max'})-[rel:KNOWS {since: 1994}]->(a:Person {name:'Hans', age:31})";
    protected static final String MULTIPLE_HOP_EDGE = "CREATE (n:Person)-[f:FRIEND_OF {since: 1995}]->(p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira'})";

    protected final TestNode MAX = TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) );
    protected final TestNode HANS = TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ) );
    protected final TestNode HANS_AGE = TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ), Pair.of( "age", 31 ) );
    protected final TestNode KIRA = TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ), Pair.of( "age", 3 ), Pair.of( "type", "dog" ) );


    @BeforeAll
    public static void start() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        createGraph();
    }


    public static void createGraph() {
        createGraph( GRAPH_NAME );
    }


    public static void createGraph( String name ) {
        execute( format( "CREATE DATABASE %s", name ) );
        execute( format( "USE GRAPH %s", name ) );
    }


    @AfterAll
    public static void tearDown() {
        deleteData( GRAPH_NAME );
    }


    public static void deleteData( String graph ) {
        execute( format( "DROP DATABASE %s IF EXISTS", graph ) );
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        AdapterCatalog adapterCatalog = Catalog.getInstance().getAdapterCatalog( 0 ).orElseThrow();
    }


    public static GraphResult execute( String query ) {
        GraphResult res = CypherConnection.executeGetResponse( query );
        if ( res.getError() != null ) {
            fail( res.getError() );
        }
        return res;
    }


    public static GraphResult execute( String query, String namespace ) {
        GraphResult res = CypherConnection.executeGetResponse( query, namespace );
        if ( res.getError() != null ) {
            fail( res.getError() );
        }
        return res;
    }


    protected boolean containsNodes( GraphResult res, boolean exclusive, TestObject... nodes ) {
        if ( res.getHeader().length == 1 && res.getHeader()[0].dataType.toLowerCase( Locale.ROOT ).contains( "node" ) ) {
            return contains( res.getData(), exclusive, 0, PolyNode.class, nodes );
        }
        throw new UnsupportedOperationException();
    }


    protected boolean containsEdges( GraphResult res, boolean exclusive, TestEdge... edges ) {
        if ( res.getHeader().length == 1 && res.getHeader()[0].dataType.toLowerCase( Locale.ROOT ).contains( "edge" ) ) {
            return contains( res.getData(), exclusive, 0, PolyEdge.class, edges );
        }
        throw new UnsupportedOperationException();
    }


    public boolean containsIn( GraphResult res, boolean exclusive, int index, TestGraphObject... expected ) {
        boolean successful = true;
        Class<? extends GraphPropertyHolder> clazz = null;
        if ( expected.length > 0 ) {
            Type type = Type.from( expected[0] );
            successful = is( res, type, index );
            clazz = (Class<? extends GraphPropertyHolder>) type.polyClass;
        }
        if ( !successful ) {
            return false;
        }

        return contains( res.data, exclusive, index, clazz, expected );
    }


    public boolean containsIn( GraphResult actual, boolean exclusive, int index, @Nullable String name, TestLiteral... expected ) {
        // simple object match
        List<PolyValue> cols = new ArrayList<>();

        for ( String[] entry : actual.getData() ) {
            cols.add( PolyValue.fromTypedJson( entry[index], PolyValue.class ) );
        }
        assert !exclusive || cols.size() == expected.length;

        boolean correct = true;
        if ( name != null ) {
            correct = actual.getHeader()[index].name.equals( name );
        }

        boolean contains = correct;
        for ( TestObject object : expected ) {
            contains &= cols.stream().anyMatch( n -> object.matches( n, exclusive ) );
        }

        return contains;

    }


    public static boolean containsRows( GraphResult actual, boolean exclusive, boolean ordered, Row... rows ) {
        try {
            List<List<PolyValue>> parsed = new ArrayList<>();

            int i = 0;
            for ( Row row : rows ) {
                parsed.add( row.asList( actual.getData()[i] ) );
                i++;
            }

            assert !exclusive || actual.getData().length >= rows.length;

            if ( ordered ) {
                return matchesExactRows( parsed, rows );
            } else {
                return matchesUnorderedRows( parsed, rows );
            }
        } catch ( Throwable t ) {
            fail( "Error while evaluating result: " + t.getMessage() );
            throw new RuntimeException();
        }
    }


    private static boolean matchesUnorderedRows( List<List<PolyValue>> parsed, Row[] rows ) {

        List<Integer> used = new ArrayList<>();
        for ( Row row : rows ) {

            int i = 0;
            boolean matches = false;
            for ( List<PolyValue> objects : parsed ) {

                if ( !matches && !used.contains( i ) ) {
                    if ( row.matches( objects ) ) {
                        used.add( i );
                        matches = true;
                    }
                }

                i++;
            }
            if ( !matches ) {
                return false;
            }
        }
        return true;

    }


    private static boolean matchesExactRows( List<List<PolyValue>> parsed, Row[] rows ) {
        boolean matches = true;
        int j = 0;
        for ( Row row : rows ) {
            matches &= row.matches( parsed.get( j ) );
            j++;
        }
        return matches;
    }


    @SneakyThrows
    private <T extends GraphPropertyHolder> boolean contains( String[][] actual, boolean exclusive, int index, Class<T> clazz, TestObject[] expected ) {
        List<T> parsed = new ArrayList<>();

        for ( String[] entry : actual ) {
            parsed.add( PolyValue.JSON_WRAPPER.readValue( entry[index], clazz ) );
        }

        assert !exclusive || parsed.size() == expected.length;

        boolean contains = true;
        for ( TestObject node : expected ) {
            contains &= parsed.stream().anyMatch( n -> node.matches( n, exclusive ) );
        }

        return contains;
    }


    public GraphResult matchAndReturnAllNodes() {
        return execute( "MATCH (n) RETURN n" );
    }


    protected void assertNode( GraphResult res, int index ) {
        assert is( res, Type.NODE, index );
    }


    protected void assertEdge( GraphResult res, int index ) {
        assert is( res, Type.EDGE, index );
    }


    protected boolean is( GraphResult res, Type type, int index ) {
        assert res.getHeader().length >= index;

        return res.getHeader()[index].dataType.toLowerCase( Locale.ROOT ).contains( type.getTypeName() );
    }


    protected void assertEmpty( GraphResult res ) {
        assert res.getData().length == 0;
    }


    @Getter
    public enum Type {
        NODE( "node", TestNode.class, PolyNode.class ),
        EDGE( "edge", TestEdge.class, PolyEdge.class ),
        PATH( "path", TestPath.class, PolyPath.class ),
        ANY( "any", TestNode.class, PolyValue.class ),
        STRING( "varchar", TestLiteral.class, PolyString.class );


        private final String typeName;
        private final Class<? extends TestObject> testClass;
        private final Class<? extends PolyValue> polyClass;


        Type( String name, Class<? extends TestObject> testClass, Class<? extends PolyValue> polyClass ) {
            this.typeName = name;
            this.testClass = testClass;
            this.polyClass = polyClass;
        }


        public static Type from( TestObject object ) {
            if ( object instanceof TestLiteral ) {
                return STRING;
            } else if ( object instanceof TestNode ) {
                return NODE;
            } else if ( object instanceof TestEdge ) {
                return EDGE;
            } else if ( object instanceof TestPath ) {
                return PATH;
            }
            throw new UnsupportedOperationException();
        }
    }


    public static class Row {

        final TestObject[] values;


        public Row( TestObject[] values ) {
            this.values = values;
        }


        public static Row of( TestObject... values ) {
            return new Row( values );
        }


        public List<PolyValue> asList( String[] actual ) {
            List<PolyValue> res = new ArrayList<>();
            assert this.values.length == actual.length;

            int i = 0;
            for ( String val : actual ) {
                res.add( this.values[i].toPoly( val ) );
                i++;
            }
            return res;
        }


        public boolean matches( List<PolyValue> objects ) {
            int i = 0;
            boolean matches = true;

            for ( PolyValue object : objects ) {
                matches &= values[i].matches( object, true );
                i++;
            }
            return matches;
        }

    }


}
