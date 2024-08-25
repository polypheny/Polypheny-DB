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

public class UnionTest extends CypherTestTemplate {

    protected static final String SINGLE_NODE_MOVIE = "CREATE (wallStreet:Movie {title: 'Wall Street' , released : 2002})";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( """
                MATCH (n:Person)
                RETURN n.name
                UNION
                MATCH (n:Person)
                RETURN n.name""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void DifferentStructureUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( """
                MATCH (p:Person)
                RETURN p.name AS name
                UNION
                MATCH (m :Movie)
                RETURN p.Title AS name
                """ );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Wall Street" ) ) );
    }


    @Test
    public void NullPropertiesUnionTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( """
                MATCH (p:Person)
                RETURN p.name AS name, p.age AS age, NULL AS title, NULL AS released
                UNION
                MATCH (m:Movie)
                RETURN NULL AS name, NULL AS age, m.title AS title, m.released AS released
                """ );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( null ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Wall Street" ), TestLiteral.from( 2002 ) ) );
    }


    @Test
    public void allUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                MATCH (n:Person)
                RETURN n.name
                UNION ALL
                MATCH (n:Person)
                RETURN n.name""" );

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

        GraphResult res = execute( """
                MATCH (p:Person)
                RETURN p.name AS name
                UNION ALL
                MATCH (m :Movie)
                RETURN p.Title AS name
                """ );

        containsRows( res, true, false,
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

        GraphResult res = execute( """
                MATCH (p:Person)
                RETURN p.name AS name, p.age AS age, NULL AS title, NULL AS released
                UNION ALL
                MATCH (m:Movie)
                RETURN NULL AS name, NULL AS age, m.title AS title, m.released AS released
                """ );

        containsRows( res, true, false,
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

        GraphResult res = execute( """
                MATCH (n:Person)
                RETURN n.name
                UNION DISTINCT
                MATCH (n:Person)
                RETURN n.name""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void DifferentStructureDistinctUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( """
                MATCH (p:Person)
                RETURN p.name AS name
                UNION DISTINCT
                MATCH (m :Movie)
                RETURN p.Title AS name
                """ );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Wall Street" ) ) );
    }


    @Test
    public void NullPropertiesDistinctUnionTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_MOVIE );

        GraphResult res = execute( """
                MATCH (p:Person)
                RETURN p.name AS name, p.age AS age, NULL AS title, NULL AS released
                UNION DISTINCT
                MATCH (m:Movie)
                RETURN NULL AS name, NULL AS age, m.title AS title, m.released AS released
                """ );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( null ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Wall Street" ), TestLiteral.from( 2002 ) ) );
    }

}
