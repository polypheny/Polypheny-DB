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

package org.polypheny.db.cypher.Subqueries;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class ExistsSubQueriesTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    public static final String EDGE_3 = "CREATE  (p:Person {name :'Max'}),(p)-[rel:OWNER_OF{ since : 2002}] -> (c:Cat : Animal {name :'Mittens' , age : 3}), (p)-[rel2:OWNER_OF { since : 1999}] -> (d:Dog :Animal { name : 'Andy' , age :10}),(d)-[:HAS_TOY]->(:Toy{name:'Banana'})";


    @Test
    public void simpleExistsSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE EXISTS {
                    (person)-[:OWNER_OF]->(:Dog)
                }
                RETURN person.name AS name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( null ) ) );
        execute( EDGE_3 );
        execute( """
                MATCH (person:Person)
                WHERE EXISTS {
                    (person)-[:OWNER_OF]->(:Dog)
                }
                RETURN person.name AS name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void whereWithExistsSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE EXISTS {
                  MATCH (person)-[:OWNER_OF]->(dog:Dog)
                  WHERE person.name = "Max"\s
                }
                RETURN dog.name AS name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Andy" ) ) );

    }


    @Test
    public void nestedExistsSubQueryTest() {

        execute( EDGE_3 );
        GraphResult res = execute( """
                MATCH (person:Person)
                WHERE EXISTS {
                  MATCH (person)-[:OWNER_OF]->(dog:Dog)
                  WHERE EXISTS {
                    MATCH (dog)-[:HAS_TOY]->(toy:Toy)
                    WHERE toy.name = 'Banana'
                  }
                }
                RETURN person.name AS name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );


    }


    @Test
    public void returnExistsSubQueryTest() {
        execute( EDGE_3 );
        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN person.name AS name, EXISTS {
                  MATCH (person)-[:OWNER_OF]->(:Dog)
                } AS hasDog""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( true ) ) );
    }


    @Test
    public void unionWithExistsSubQueryTest() {
        execute( EDGE_3 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                MATCH (person:Person)
                RETURN
                    person.name AS name,
                    EXISTS {
                        MATCH (person)-[:HAS_DOG]->(:Dog)
                        UNION
                        MATCH (person)-[:HAS_CAT]->(:Cat)
                    } AS hasPet""" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( true ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( false ) ) );
    }


    @Test
    public void withClauseWithExistsSubQueryTest() {
        execute( EDGE_3 );
        GraphResult res = execute( """
                MATCH (person:Person {name: name})
                WHERE EXISTS {
                    WITH "Andy" AS name
                    MATCH (person)-[:OWNER_OF]->(d:Dog)
                    WHERE d.name = name
                }
                RETURN person.name AS name""" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


}
