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

package org.polypheny.db.cypher.subqueries;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;


public class CountSubQueriesTest extends CypherTestTemplate {

    public static final String EDGE_3 = "CREATE  (p:Person {name :'Max'}),(p)-[rel:OWNER_OF{ since : 2002}] -> (c:Cat : Animal {name :'Mittens' , age : 3}), (p)-[rel2:OWNER_OF { since : 1999}] -> (d:Dog :Animal { name : 'Andy' , age :10})";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleCountSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE COUNT { (person)-[r:OWNER_OF]->(:Animal) } > 1
                RETURN person.name AS name""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void useCountSubQueryInReturnTest() {
        execute( EDGE_3 );

        GraphResult res = execute( "MATCH (person:Person) RETURN person.name, COUNT { (person)-[:OWNER_OF]->(:Dog) } as howManyDogs" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 1 ) ) );
    }


    @Test
    public void whereWithCountSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE COUNT {
                  (person)-[r:OWNER_OF]->(dog:Dog)
                  WHERE person.name = dog.name } = 1
                RETURN person.name AS name""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void unionWithCountSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN
                    person.name AS name,
                    COUNT {
                        MATCH (person)-[:OWNER_OF]->(dog:Dog)
                        RETURN dog.name AS petName
                        UNION
                        MATCH (person)-[:OWNER_OF]->(cat:Cat)
                        RETURN cat.name AS petName
                    } AS numPets""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 2 ) ) );
    }


    @Test
    public void withClauseWithCountSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE COUNT {
                    WITH "Andy" AS dogName
                    MATCH (person)-[:OWNER_OF]->(d:Dog)
                    WHERE d.name = dogName
                } = 1
                RETURN person.name AS name""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void updateWithCountSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person) WHERE person.name ="Max"
                SET person.howManyDogs = COUNT { (person)-[:OWNER_OF]->(:Dog) }
                RETURN person.howManyDogs as howManyDogs""" );

        containsRows( res, true, false, Row.of(
                TestLiteral.from( 1 ) ) );
    }


    @Test
    public void caseWithCountSubQueryTest() {
        execute( EDGE_3 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN
                   CASE
                     WHEN COUNT { (person)-[:OWNER_OF]->(:Dog) } >= 1 THEN "DogLover " + person.name
                     ELSE person.name
                   END AS result""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "DogLover" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );
    }

}
