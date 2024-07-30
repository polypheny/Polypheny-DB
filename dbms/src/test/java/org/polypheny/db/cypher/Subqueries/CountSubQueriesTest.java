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

import org.bouncycastle.crypto.modes.G3413CBCBlockCipher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class CountSubQueriesTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    public static final String EDGE_3 = "CREATE  (p:Person {name :'Max'}),(p)-[rel:OWNER_OF{ since : 2002}] -> (c:Cat : Animal {name :'Mittens' , age : 3}), (p)-[rel2:OWNER_OF { since : 1999}] -> (d:Dog :Animal { name : 'Andy' , age :10})";


    @Test
    public void simpleCountSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        execute( EDGE_3 );

        GraphResult res = execute( "MATCH (person:Person)\n"
                + "WHERE COUNT { (person)-[r:OWNER_OF]->(:Animal) } > 1\n"
                + "RETURN person.name AS name" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void useCountSubQueryInReturnTest() {
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN person.name, COUNT { (person)-[:OWNER_OF]->(:Dog) } as howManyDogs" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 1 ) ) );

    }


    @Test
    public void whereWithCountSubQueryTest() {
        execute( SINGLE_NODE_PERSON_2 );
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "WHERE COUNT {\n"
                + "  (person)-[r:OWNER_OF]->(dog:Dog)\n"
                + "  WHERE person.name = dog.name } = 1\n"
                + "RETURN person.name AS name" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void UnionWithCountSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN\n"
                + "    person.name AS name,\n"
                + "    COUNT {\n"
                + "        MATCH (person)-[:OWNER_OF]->(dog:Dog)\n"
                + "        RETURN dog.name AS petName\n"
                + "        UNION\n"
                + "        MATCH (person)-[:OWNER_OF]->(cat:Cat)\n"
                + "        RETURN cat.name AS petName\n"
                + "    } AS numPets" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 2 ) ) );
    }


    @Test
    public void WithClauseWithCountSubQueryTest() {
        execute( EDGE_3 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "WHERE COUNT {\n"
                + "    WITH \"Andy\" AS dogName\n"
                + "    MATCH (person)-[:OWNER_OF]->(d:Dog)\n"
                + "    WHERE d.name = dogName\n"
                + "} = 1\n"
                + "RETURN person.name AS name" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );

    }


    @Test
    public void UpdateWithCountSubQueryTest() {
        execute( EDGE_3 );

        GraphResult res = execute( "MATCH (person:Person) WHERE person.name =\"Max\"\n"
                + "SET person.howManyDogs = COUNT { (person)-[:OWNER_OF]->(:Dog) }\n"
                + "RETURN person.howManyDogs as howManyDogs" );

        assert containsRows( res, true, false, Row.of(
                TestLiteral.from( 1 ) ) );


    }


    @Test
    public void CaseWithCountSubQueryTest() {
        execute( EDGE_3 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (person:Person)\n"
                + "RETURN\n"
                + "   CASE\n"
                + "     WHEN COUNT { (person)-[:OWNER_OF]->(:Dog) } >= 1 THEN \"DogLover \" + person.name\n"
                + "     ELSE person.name\n"
                + "   END AS result" );
        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "DogLover" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );


    }


}
