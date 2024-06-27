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

    @Test
    public void simpleUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2);
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2);
        execute(SINGLE_NODE_ANIMAL);
        execute( SINGLE_NODE_ANIMAL);


        GraphResult res =  execute( "MATCH (n:Actor)\n"
                + "RETURN n.name AS name\n"
                + "UNION \n"
                + "MATCH (n:Movie)\n"
                + "RETURN n.title AS name" );



        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ),
                Row.of( TestLiteral.from( "Kira" ) ));
    }
    @Test
    public void allUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2);
        execute(SINGLE_NODE_ANIMAL);
        execute( SINGLE_NODE_ANIMAL);

        GraphResult res = execute( "MATCH (n:Actor)\n"
                + "RETURN n.name AS name\n"
                + "UNION ALL\n"
                + "MATCH (n:Movie)\n"
                + "RETURN n.title AS name" );


        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ),
                Row.of( TestLiteral.from( "Kira" ) ),
                Row.of( TestLiteral.from( "Kira" ) ) );


    }





    @Test
    public void distinctUnionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2);
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2);
        execute(SINGLE_NODE_ANIMAL);
        execute( SINGLE_NODE_ANIMAL);

        GraphResult res = execute( "MATCH (n:Actor)\n"
                + "RETURN n.name AS name\n"
                + "UNION DISTINCT\n"
                + "MATCH (n:Movie)\n"
                + "RETURN n.title AS name" );


        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ),
                Row.of( TestLiteral.from( "Kira" ) ));

    }

}
