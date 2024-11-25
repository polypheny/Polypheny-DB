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

package org.polypheny.db.cypher.clause.write;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;


public class ForeachTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void createWithForeachTest() {
        execute( """
                WITH ['Alice', 'Bob', 'Charlie'] AS names
                FOREACH (name IN names |
                    CREATE (p:Person {name: name})
                )""" );

        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 3, res.getData().length );
        containsNodes( res, true,
                TestNode.from( Pair.of( "name", "Alice" ) ),
                TestNode.from( Pair.of( "name", "Bob" ) ),
                TestNode.from( Pair.of( "name", "Charlie" ) ) );
    }


    @Test
    public void mergeWithForeachTest() {
        execute( """
                WITH ['Alice', 'Bob', 'Charlie'] AS names
                FOREACH (name IN names |
                    MERGE (p:Person {name: name})
                )""" );

        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 3, res.getData().length );
        containsNodes( res, true,
                TestNode.from( Pair.of( "name", "Alice" ) ),
                TestNode.from( Pair.of( "name", "Bob" ) ),
                TestNode.from( Pair.of( "name", "Charlie" ) ) );
    }


    @Test
    public void deleteWithForeachTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( """
                MATCH (p:Person)
                WITH collect(p) AS people
                FOREACH (p IN people |
                    DELETE p
                )""" );
        GraphResult res = matchAndReturnAllNodes();
        assertEmpty( res );
    }


    @Test
    public void removeWithForeachTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( """
                MATCH (p:Person)
                WITH collect(p) AS people
                FOREACH (p IN people |
                    REMOVE p.name\s
                )""" );

        GraphResult res = matchAndReturnAllNodes();
        containsRows( res, true, false, Row.of( TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void updateWithForeachTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( """
                MATCH (p:Person)
                WITH collect(p) AS people
                FOREACH (p IN people |
                    SET p.status = 'active'
                )""" );

        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ), Pair.of( "status", "active" ) ),
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ), Pair.of( "status", "active" ) ) );
    }


    @Test
    public void nestedForeachTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( """
                MATCH (p:Person)
                WITH collect(p) AS people
                FOREACH (p1 IN people |
                    FOREACH (p2 IN people |
                        CREATE (p1)-[:KNOWS]->(p2)
                    )
                )""" );
        GraphResult res = execute( "MATCH (p1)-[r:KNOWS]->(p2) RETURN r" );
        assertEquals( 4, res.getData().length );
        res = execute( "MATCH (p1)-[r:KNOWS]-(p2) RETURN r" );
        assertEquals( 6, res.getData().length );
    }

}
