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
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;
import java.util.List;

public class CollectSubQueriesTest extends CypherTestTemplate {


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    public static final String EDGE_3 = "CREATE  (p:Person {name :'Max'}),(p)-[rel:OWNER_OF{ since : 2002}] -> (c:Cat : Animal {name :'Mittens' , age : 3}), (p)-[rel2:OWNER_OF { since : 1999}] -> (d:Dog :Animal { name : 'Andy' , age :10})";


    @Test
    public void simpleCollectSubQueryTest() {
        execute( SINGLE_EDGE_1 );

        GraphResult res = execute( "MATCH (person:Person)\n"
                + "WHERE 'Kira' IN COLLECT { MATCH (person)-[:OWNER_OF]->(a:Animal) RETURN a.name }\n"
                + "RETURN person.name AS name" );

        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ) ) );

    }


    @Test
    public void useCollectSubQueryInReturnTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN person.name,\n"
                + "       COLLECT {\n"
                + "            MATCH (person)-[:OWNER_OF]->(d:Dog)\n"
                + "            RETURN d.name\n"
                + "       } as DogNames" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Andy" ) ) );

    }


    @Test
    public void whereWithCollectSubQueryTest() {
        execute( EDGE_3 );
        GraphResult res = execute( "\n"
                + "MATCH (person:Person)\n"
                + "RETURN person.name as name, COLLECT {\n"
                + "  MATCH (person)-[r:OWNER_OF]->(a:Dog)\n"
                + "  WHERE a.age <= 3\n"
                + "  RETURN a.name\n"
                + "} as youngDog \n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Andy" ) ) );

    }


    @Test
    public void unionWithCollectSubQueryTest() {
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN\n"
                + "    person.name AS name,\n"
                + "    COLLECT {\n"
                + "        MATCH (person)-[:HAS_DOG]->(dog:Dog)\n"
                + "        RETURN dog.name AS petName\n"
                + "        UNION\n"
                + "        MATCH (person)-[:HAS_CAT]->(cat:Cat)\n"
                + "        RETURN cat.name AS petName\n"
                + "    } AS petNames" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( List.of( "Andy", "Mittens" ) ) ) );


    }


    @Test
    public void withClauseWithCollectSubQueryTest() {
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN person.name AS name, COLLECT {\n"
                + "    WITH 1999 AS yearOfTheDog\n"
                + "    MATCH (person)-[r:OWNER_OF]->(d:Dog)\n"
                + "    WHERE r.since = yearOfTheDog\n"
                + "    RETURN d.name\n"
                + "} as dogsOfTheYear" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Andy" ) ) );
    }


    @Test
    public void caseWithCollectSubQueryTest() {
        execute( EDGE_3 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN\n"
                + "   CASE\n"
                + "     WHEN COLLECT { MATCH (person)-[:OWNER_OF]->(d:Dog) RETURN d.name } = []  THEN \" No Dogs \" + person.name\n"
                + "     ELSE person.name\n"
                + "   END AS result" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "No Dogs" ) ),
                Row.of( TestLiteral.from( "Max" ) ) );

    }


    @Test
    public void updateWithCollectSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)"
                + " WHERE person.name = \"Hans\"\n"
                + "SET person.dogNames = COLLECT { MATCH (person)-[:OWNER_OF]->(d:Dog) RETURN d.name }\n"
                + "RETURN person.dogNames as dogNames" );

        assert containsRows( res , true , false ,
        Row.of( TestLiteral.from( List.of("Andy") ) )       );
    }



}
