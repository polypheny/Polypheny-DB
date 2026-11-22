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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

public class DmlUpdateTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void updatePropertyTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a.age = 25" );

        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ), Pair.of( "age", 25 ) ) ) );
    }


    @Test
    public void updateLabelTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a:Swiss" );

        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person", "Swiss" ), Pair.of( "name", "Max" ) ) ) );
    }


    @Test
    public void updateLabelsTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a:Swiss:German" );

        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person", "Swiss", "German" ), Pair.of( "name", "Max" ) ) ) );
    }


    @Test
    public void updateVariablesReplaceTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a = {} " );

        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person" ) ) ) );
    }


    @Test
    public void updateVariablesIncrementTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a = { age: 13, job: 'Developer'} " );

        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from(
                        List.of( "Person" ),
                        Pair.of( "age", 13 ),
                        Pair.of( "job", "Developer" ) ) ) );
    }


    @Test
    void invalidSetValuesRollBackThePrecedingCreate() {
        for ( String assignment : List.of(
                "n.value = [{num:1}]",
                "n.value = {num:1}",
                "n.value = [[1]]",
                "n.value = [1,null]",
                "n.value = [1,'one']",
                "n += {valid:2, invalid:[{num:1}]}",
                "n = {valid:2, invalid:[{num:1}]}" ) ) {
            assertInvalidPropertyType( "CREATE (n:InvalidSet) SET " + assignment );
            assertEquals( 0, value( "MATCH (n:InvalidSet) RETURN count(n)" ).asNumber().intValue(), assignment );
        }
    }


    @Test
    void failedSetLeavesOtherAssignmentsAndInputRowsUnchanged() {
        execute( "CREATE (:Original {marker:1})-[:ORIGINAL {marker:1}]->(:Other)" );

        assertInvalidPropertyType( """
                MATCH (:Original)-[r:ORIGINAL]->(:Other)
                SET r.marker = 2, r.bad = [{num:1}]
                """ );
        assertEquals( 1, value( "MATCH ()-[r:ORIGINAL]->() RETURN r.marker" ).asNumber().intValue() );

        assertInvalidPropertyType( """
                MATCH (n:Original)
                UNWIND [{rank:0, value:[1]}, {rank:1, value:[{num:1}]}] AS row
                WITH n, row.value AS value ORDER BY row.rank
                SET n.marker = 2, n.value = value
                """ );
        assertEquals( 1, value( "MATCH (n:Original) RETURN n.marker" ).asNumber().intValue() );
        assertTrue( value( "MATCH (n:Original) RETURN n.value IS NULL" ).asBoolean().value );

        assertInvalidPropertyType( """
                MATCH (n:Original) SET n.marker = 2
                WITH n, {marker:3, invalid:[{num:1}]} AS props
                SET n = props
                """ );
        assertEquals( 1, value( "MATCH (n:Original) RETURN n.marker" ).asNumber().intValue() );
    }


    @Test
    void setPropertyMapsCanBeComputedAndKeepNullRemovalSemantics() {
        execute( "CREATE (:Original {marker:1, dropMe:2})" );
        execute( """
                MATCH (n:Original)
                WITH n, {dropMe:null, values:[1,2.5], marker:n.marker + 1} AS props
                SET n += props
                """ );
        assertTrue( value( "MATCH (n:Original) RETURN n.dropMe IS NULL" ).asBoolean().value );
        assertEquals( 2, value( "MATCH (n:Original) RETURN n.marker" ).asNumber().intValue() );
        assertEquals( 2.5, value( "MATCH (n:Original) RETURN n.values[1]" ).asNumber().doubleValue() );

        execute( "MATCH (n:Original) SET n = n" );
        assertEquals( 2, value( "MATCH (n:Original) RETURN n.marker" ).asNumber().intValue() );

        execute( "MATCH (n:Original) WITH n, {empty:[]} AS props SET n = props" );
        assertTrue( value( "MATCH (n:Original) RETURN n.marker IS NULL" ).asBoolean().value );
        assertTrue( value( "MATCH (n:Original) RETURN n.empty" ).asList().isEmpty() );
    }


    private static void assertInvalidPropertyType( String query ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.contains( "InvalidPropertyType" ), error );
    }


    private static PolyValue value( String query ) {
        return PolyValue.fromTypedJson( execute( query ).getData()[0][0], PolyValue.class );
    }


    @Test
    @Disabled // Extension of Cypher implementation required
    public void updatePropertyReturnTest() {
        execute( "MATCH (a:Animal {name: 'Kira'})\n"
                + "SET a.age = 4\n"
                + "RETURN a" );
    }


    @Test
    @Disabled // Extension of Cypher implementation required
    public void updateRelationshipExistingPropertyTest() {
        execute( "MATCH (:Person {name:'Max Muster'})-[rel:OWNER_OF]->(a:Animal {name: 'Kira'})\n"
                + "SET rel.since = 2018" );
    }


    @Test
    @Disabled // Extension of Cypher implementation required
    public void updateRelationshipNewPropertyTest() {
        execute( "MATCH (:Person {name:'Max Muster'})-[rel:OWNER_OF]->(a:Animal {name: 'Kira'})\n"
                + "SET rel.status = 'fresh'" );
    }

}
