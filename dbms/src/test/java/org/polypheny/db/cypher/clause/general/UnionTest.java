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

package org.polypheny.db.cypher.clause.general;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;
import java.util.Arrays;

public class UnionTest extends CypherTestTemplate {


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    protected static final String SINGLE_NODE_MOVIE = "CREATE (wallStreet:Movie {title: 'Wall Street' , released : 2002})";


    @Test
    public void simpleUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n:Person)\n"
                + "RETURN n.name \n"
                + "UNION \n"
                + "MATCH (n:Person)\n"
                + "RETURN n.name " );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void DifferentStructureUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( "MATCH (p:Person)\n"
                + "RETURN p.name AS name\n"
                + "UNION\n"
                + "MATCH (m :Movie)\n"
                + "RETURN p.Title AS name\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Wall Street" ) ) );
    }


    @Test
    public void NullPropertiesUnionTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( "MATCH (p:Person)\n"
                + "RETURN p.name AS name, p.age AS age, NULL AS title, NULL AS released\n"
                + "UNION\n"
                + "MATCH (m:Movie)\n"
                + "RETURN NULL AS name, NULL AS age, m.title AS title, m.released AS released\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( null ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Wall Street" ), TestLiteral.from( 2002 ) ) );
    }


    @Test
    public void allUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person)\n"
                + "RETURN n.name \n"
                + "UNION ALL \n"
                + "MATCH (n:Person)\n"
                + "RETURN n.name " );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ),
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );


    }


    @Test
    public void DifferentStructureAllUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_MOVIE );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( "MATCH (p:Person)\n"
                + "RETURN p.name AS name\n"
                + "UNION ALL\n"
                + "MATCH (m :Movie)\n"
                + "RETURN p.Title AS name\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Wall Street" ) ),
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Wall Street" ) ) );
    }


    @Test
    public void NullPropertiesAllUnionTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_MOVIE );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( "MATCH (p:Person)\n"
                + "RETURN p.name AS name, p.age AS age, NULL AS title, NULL AS released\n"
                + "UNION ALL\n"
                + "MATCH (m:Movie)\n"
                + "RETURN NULL AS name, NULL AS age, m.title AS title, m.released AS released\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( null ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( null ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Wall Street" ), TestLiteral.from( 2002 ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Wall Street" ), TestLiteral.from( 2002 ) ) );
    }


    @Test
    public void distinctUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person)\n"
                + "RETURN n.name \n"
                + "UNION DISTINCT  \n"
                + "MATCH (n:Person)\n"
                + "RETURN n.name " );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void DifferentStructureDistinctUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( "MATCH (p:Person)\n"
                + "RETURN p.name AS name\n"
                + "UNION DISTINCT\n"
                + "MATCH (m :Movie)\n"
                + "RETURN p.Title AS name\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Wall Street" ) ) );
    }


    @Test
    public void NullPropertiesDistinctUnionTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( "MATCH (p:Person)\n"
                + "RETURN p.name AS name, p.age AS age, NULL AS title, NULL AS released\n"
                + "UNION DISTINCT \n"
                + "MATCH (m:Movie)\n"
                + "RETURN NULL AS name, NULL AS age, m.title AS title, m.released AS released\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( null ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Wall Street" ), TestLiteral.from( 2002 ) ) );
    }

}