/*
 * Copyright 2019-2026 The Polypheny Project
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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;


class CypherExistsTest extends CypherTestTemplate {

    @BeforeEach
    void createFixture() {
        tearDown();
        createGraph();
        execute( """
                CREATE (a:Person {name:'Ada', enabled:false})-[:KNOWS {score:0}]->(b:Person {name:'Lin'}),
                       (a)-[:KNOWS {score:2}]->(:Person {name:'Sam'}),
                       (:Person {name:'Isolated'}),
                       (b)-[:SELF]->(b)
                """ );
    }


    @Test
    void testsNodeRelationshipAndMapProperties() {
        GraphResult found = execute( """
                MATCH (a:Person {name:'Ada'})-[r:KNOWS {score:0}]->(b)
                WITH a, r, b, {active:false, zero:0, empty:'', nil:null} AS values
                RETURN exists(a.enabled), EXISTS(r.score), exists(b.enabled), exists(r.missing),
                       exists(values.active), exists(values.zero), exists(values.empty), exists(values.nil), exists(values.missing)
                """ );
        assertEquals( List.of( true, true, false, false, true, true, true, false, false ), flags( found ) );

        GraphResult where = execute( """
                MATCH (n:Person) WHERE exists(n.enabled)
                RETURN n.name
                """ );
        assertEquals( 1, where.getData().length );
        assertEquals( "Ada", cell( where, 0, 0 ).asString().value );
    }


    @Test
    void absentOptionalBindingsReturnFalse() {
        GraphResult result = execute( """
                MATCH (origin:Person {name:'Ada'})
                OPTIONAL MATCH (origin)-[r:MISSING]->(end)
                RETURN exists(r.score), exists(end.name), exists((end)-[:KNOWS]->())
                """ );
        assertEquals( List.of( false, false, false ), flags( result ) );
    }


    @Test
    void evaluatesCorrelatedPathsWithDirectionLengthAndProperties() {
        GraphResult result = execute( """
                MATCH (a:Person {name:'Ada'}), (b:Person {name:'Lin'})
                RETURN exists((a)-[:KNOWS {score:0}]->(b)),
                       exists((a)<-[:KNOWS]-(b)),
                       exists((a)-[:KNOWS|SELF*2]->(b)),
                       exists((a)-[:KNOWS {score:abs(-2)}]->(:Person {name:'Sam'})),
                       exists((a)-[:KNOWS]->(:Person {name:b.name})),
                       exists((b {name:'Lin'})-[:SELF]->(b {name:'Sam'}))
                """ );
        assertEquals( List.of( true, false, true, true, true, false ), flags( result ) );
    }


    @Test
    void preservesOptionalMultiplicityAndPatternPredicateScoping() {
        GraphResult where = execute( """
                MATCH (a:Person)
                WHERE (a)-[:KNOWS]->(:Person {name:'Lin'}) AND NOT exists((a)-[:SELF]->())
                RETURN a.name
                """ );
        assertEquals( 1, where.getData().length );
        assertEquals( "Ada", cell( where, 0, 0 ).asString().value );

        GraphResult optional = execute( """
                MATCH (a:Person {name:'Ada'})
                OPTIONAL MATCH (a)-[:KNOWS]->(b)
                WHERE exists((b)-[:SELF]->())
                RETURN b.name
                """ );
        assertEquals( 1, optional.getData().length );
        assertEquals( "Lin", cell( optional, 0, 0 ).asString().value );

        GraphResult noMatch = execute( """
                MATCH (a:Person {name:'Ada'})
                OPTIONAL MATCH (a)-[:KNOWS]->(b)
                WHERE exists((b)-[:MISSING]->())
                RETURN exists(b.name)
                """ );
        assertEquals( List.of( false ), flags( noMatch ) );
    }


    @Test
    void resolvesPathBindingsInProjectionsAndReadsOwnWrites() {
        GraphResult projected = execute( """
                MATCH p=(a:Person {name:'Ada'})-[r:KNOWS {score:0}]->(b)
                RETURN exists((a)-[r]->(b)),
                       CASE WHEN exists((b)-[:SELF]->(b)) THEN true ELSE false END
                """ );
        assertEquals( List.of( true, true ), flags( projected ) );
        GraphResult created = execute( """
                CREATE (n:New)-[:FRESH]->()
                RETURN exists((n)-[:FRESH]->())
                """ );
        assertEquals( List.of( true ), flags( created ) );
        GraphResult updated = execute( """
                MATCH (n:New)
                SET n.hasRelationship = exists((n)-[:FRESH]->())
                RETURN n.hasRelationship
                """ );
        assertEquals( List.of( true ), flags( updated ) );
    }


    @Test
    void rejectsInvalidArgumentsAndUnknownPatternVariables() {
        assertError( "RETURN exists()", "exactly one" );
        assertError( "RETURN exists(true)", "property or path pattern" );
        assertError( "WITH 1 AS value RETURN exists(value.test)", "requires a property" );
        assertError( "RETURN exists(date('2024-02-01').year)", "requires a property" );
        assertError( "RETURN exists((unknown)-[:KNOWS]->())", "variable" );
    }


    @Test
    void requiresExplicitExistsWhenProjectingPatterns() {
        assertError( "MATCH (n:Person) RETURN (n)-[:KNOWS]->()", "use exists(pattern)" );
        assertError( "MATCH (n:Person) WITH (n)-[:KNOWS]->() AS pattern RETURN pattern", "use exists(pattern)" );
        GraphResult matches = execute( """
                MATCH (n:Person)
                WHERE (n)-[:SELF]->()
                RETURN n.name AS name
                """ );
        assertEquals( 1, matches.getData().length );
        assertEquals( "Lin", cell( matches, 0, 0 ).asString().value );
    }


    @Test
    void evaluatesSubqueryCardinalityInsteadOfProjectedValues() {
        GraphResult result = execute( """
                RETURN EXISTS { RETURN false } AS present,
                       EXISTS { MATCH (:Missing) RETURN true } AS absent,
                       EXISTS { MATCH (:Missing) RETURN count(*) } AS aggregatesOnEmptyInput
                """ );
        assertEquals( List.of( true, false, true ), flags( result ) );
    }


    @Test
    void preservesBothScopesAndOptionalRows() {
        GraphResult result = execute( """
                MATCH (a:Person {name:'Ada'})
                OPTIONAL MATCH (b:Person)
                WHERE EXISTS { (a)-[:KNOWS]->(b) WHERE EXISTS { (b)-[:SELF]->() } }
                RETURN b.name AS matched
                """ );
        assertEquals( 1, result.getData().length );
        assertEquals( "Lin", cell( result, 0, 0 ).asString().value );

        GraphResult unmatched = execute( """
                MATCH (a:Person {name:'Ada'})
                OPTIONAL MATCH (b:Person)
                WHERE EXISTS { (a)-[:MISSING]->(b) }
                RETURN EXISTS { (b)-[:SELF]->() } AS present
                """ );
        assertEquals( List.of( false ), flags( unmatched ) );
        assertError( "MATCH (a) WHERE EXISTS { (a)-->(local) } RETURN local", "variable" );
    }


    private static void assertError( String query, String message ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.toLowerCase().contains( message ), error );
    }


    private static List<Boolean> flags( GraphResult result ) {
        assertEquals( 1, result.getData().length );
        List<Boolean> flags = new ArrayList<>();
        for ( int i = 0; i < result.getData()[0].length; i++ ) {
            flags.add( cell( result, 0, i ).asBoolean().value );
        }
        return flags;
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        return PolyValue.fromTypedJson( result.getData()[row][column], PolyValue.class );
    }

}
