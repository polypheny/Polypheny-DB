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

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;


public class CollectSubQueriesTest extends CypherTestTemplate {

    public static final String EDGE_3 = "CREATE  (p:Person {name :'Max'}),(p)-[rel:OWNER_OF{ since : 2002}] -> (c:Cat : Animal {name :'Mittens' , age : 3}), (p)-[rel2:OWNER_OF { since : 1999}] -> (d:Dog :Animal { name : 'Andy' , age :10})";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleCollectSubQueryTest() {
        execute( SINGLE_EDGE_1 );

        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE 'Kira' IN COLLECT { MATCH (person)-[:OWNER_OF]->(a:Animal) RETURN a.name }
                RETURN person.name AS name""" );

        containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void useCollectSubQueryInReturnTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN person.name,
                       COLLECT {
                            MATCH (person)-[:OWNER_OF]->(d:Dog)
                            RETURN d.name
                       } as DogNames""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Andy" ) ) );
    }


    @Test
    public void whereWithCollectSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN person.name as name, COLLECT {
                  MATCH (person)-[r:OWNER_OF]->(a:Dog)
                  WHERE a.age <= 3
                  RETURN a.name
                } as youngDog """ );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Andy" ) ) );
    }


    @Test
    public void unionWithCollectSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN
                    person.name AS name,
                    COLLECT {
                        MATCH (person)-[:HAS_DOG]->(dog:Dog)
                        RETURN dog.name AS petName
                        UNION
                        MATCH (person)-[:HAS_CAT]->(cat:Cat)
                        RETURN cat.name AS petName
                    } AS petNames""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( List.of( "Andy", "Mittens" ) ) ) );
    }


    @Test
    public void withClauseWithCollectSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN person.name AS name, COLLECT {
                    WITH 1999 AS yearOfTheDog
                    MATCH (person)-[r:OWNER_OF]->(d:Dog)
                    WHERE r.since = yearOfTheDog
                    RETURN d.name
                } as dogsOfTheYear""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Andy" ) ) );
    }


    @Test
    public void caseWithCollectSubQueryTest() {
        execute( EDGE_3 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN
                   CASE
                     WHEN COLLECT { MATCH (person)-[:OWNER_OF]->(d:Dog) RETURN d.name } = []  THEN " No Dogs " + person.name
                     ELSE person.name
                   END AS result""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "No Dogs" ) ),
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void updateWithCollectSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        execute( EDGE_3 );

        GraphResult res = execute( """
                MATCH (person:Person) WHERE person.name = "Hans"
                SET person.dogNames = COLLECT { MATCH (person)-[:OWNER_OF]->(d:Dog) RETURN d.name }
                RETURN person.dogNames as dogNames""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( List.of( "Andy" ) ) ) );
    }

}
