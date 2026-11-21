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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherFunctionsTest extends CypherTestTemplate {

    @Test
    void countsListElementsAndUnicodeCodePoints() {
        GraphResult result = execute( """
                RETURN size([1, null, [2, 3]]) AS listCount, size([]) AS emptyList,
                       size('a😀é') AS unicodeLength, size('') AS emptyString,
                       size(null) AS absent, size(range(1, 4)) AS generatedCount
                """ );

        assertEquals( 3, cell( result, 0 ).asNumber().longValue() );
        assertEquals( 0, cell( result, 1 ).asNumber().longValue() );
        assertEquals( 3, cell( result, 2 ).asNumber().longValue() );
        assertEquals( 0, cell( result, 3 ).asNumber().longValue() );
        assertNull( cell( result, 4 ) );
        assertEquals( 4, cell( result, 5 ).asNumber().longValue() );
        assertError( "MATCH p=(:Missing)-->() RETURN size(p)", "size() requires a list or string" );
        assertError( "UNWIND [{x: 7}] AS m RETURN size(m.x)", "size() requires a list or string" );
    }


    @Test
    void createsInclusiveIntegerRangesInEitherDirection() {
        GraphResult result = execute( """
                RETURN range(-1, 1) AS ascending, range(4, -1, -2) AS descending,
                       range(1, 0) AS incompatibleDirection, range(7, 7, -2) AS singleton,
                       range(null, 5) AS absentStart, range(1, null) AS absentEnd,
                       range(1, 5, null) AS absentStep
                """ );
        assertEquals( List.of( -1L, 0L, 1L ), integers( result, 0 ) );
        assertEquals( List.of( 4L, 2L, 0L ), integers( result, 1 ) );
        assertEquals( List.of(), integers( result, 2 ) );
        assertEquals( List.of( 7L ), integers( result, 3 ) );
        assertNull( cell( result, 4 ) );
        assertNull( cell( result, 5 ) );
        assertNull( cell( result, 6 ) );

        GraphResult unwind = execute( "UNWIND range(1, 5, 2) AS x RETURN x ORDER BY x" );
        assertEquals( 3, unwind.getData().length );
        assertEquals( 5, integer( unwind.getData()[2][0] ) );
    }


    @Test
    void handlesRangeOverflowAndRejectsInvalidArguments() {
        GraphResult result = execute( """
                RETURN range(9223372036854775806, 9223372036854775807) AS nearMax,
                       range(-9223372036854775807, -9223372036854775808, -1) AS nearMin,
                       range(9223372036854775807, -9223372036854775808, -9223372036854775808) AS wide
                """ );
        assertEquals( List.of( Long.MAX_VALUE - 1, Long.MAX_VALUE ), integers( result, 0 ) );
        assertEquals( List.of( Long.MIN_VALUE + 1, Long.MIN_VALUE ), integers( result, 1 ) );
        assertEquals( List.of( Long.MAX_VALUE, -1L ), integers( result, 2 ) );
        assertError( "RETURN range(3)", "requires two or three arguments" );
        assertEquals( 0, execute( "MATCH (:Missing) RETURN range(1.0, 5)" ).getData().length );
        assertError( "RETURN range(1.0, 5)", "requires integer arguments" );
        assertError( "UNWIND [{x: '1'}] AS m RETURN range(m.x, 5)", "requires integer arguments" );
        assertError( "RETURN range(1, 5, 0)", "requires a non-zero step" );
        assertError( "RETURN range(0, 9223372036854775807)", "too large to represent" );
    }


    @Test
    void exposesRelationshipTypeAndPersistentEntityIdentity() {
        execute( """
                CREATE (:FunctionNode {nodeKey:'source'})-[:FUNCTION_LINK {edgeKey:'link'}]->(:FunctionNode {nodeKey:'target'})
                """ );
        GraphResult result = execute( """
                MATCH (a:FunctionNode)-[r:FUNCTION_LINK]->(b)
                RETURN a, r, b, id(a) AS sourceId, id(r) AS relationshipId, id(b) AS targetId, type(r) AS relationshipType
                """ );
        String sourceId = cell( result, 3 ).asString().value;
        String relationshipId = cell( result, 4 ).asString().value;
        assertEquals( cell( result, 0 ).asNode().getId().value, sourceId );
        assertEquals( cell( result, 1 ).asEdge().getId().value, relationshipId );
        assertEquals( cell( result, 2 ).asNode().getId().value, cell( result, 5 ).asString().value );
        assertNotEquals( sourceId, cell( result, 5 ).asString().value );
        assertEquals( "FUNCTION_LINK", cell( result, 6 ).asString().value );

        GraphResult matching = execute( """
                MATCH (a:FunctionNode)-[r]->()
                WHERE id(a) = '%s' AND type(r) = 'FUNCTION_LINK'
                RETURN id(r) AS relationshipId
                """.formatted( sourceId ) );
        assertEquals( relationshipId, cell( matching, 0 ).asString().value );

        GraphResult missing = execute( """
                OPTIONAL MATCH (n:FunctionMissing)-[r]->()
                RETURN type(r) AS relationshipType, id(n) AS nodeId, id(r) AS relationshipId
                """ );
        assertNull( cell( missing, 0 ) );
        assertNull( cell( missing, 1 ) );
        assertNull( cell( missing, 2 ) );
        assertError( "MATCH (n:FunctionMissing) RETURN type(n)", "requires a relationship" );
        assertError( "RETURN id('not an entity')", "requires a node or relationship" );
    }


    @Test
    void exposesNodeLabelsAndPreservesAnAbsentNode() {
        execute( "CREATE (:FunctionLabels:Secondary)-[:FUNCTION_LABEL_LINK]->()" );
        GraphResult result = execute( """
                MATCH (n:FunctionLabels)-[:FUNCTION_LABEL_LINK]->(unlabeled)
                OPTIONAL MATCH (missing:FunctionMissing)
                RETURN labels(n) AS labels, labels(unlabeled) AS unlabeled, labels(missing) AS missing
                """ );
        assertEquals( List.of( "FunctionLabels", "Secondary" ),
                cell( result, 0 ).asList().stream().map( label -> label.asString().value ).sorted().toList() );
        assertEquals( List.of(), cell( result, 1 ).asList() );
        assertNull( cell( result, 2 ) );
        assertError( "MATCH ()-[r:FUNCTION_LABEL_LINK]->() RETURN labels(r)", "labels() requires a node" );
        assertError( "UNWIND [{x:'a'}] AS m RETURN labels(m.x)", "labels() requires a node" );
    }


    @Test
    void aggregatesSampleAndPopulationDeviationWithDistinctAndNulls() {
        GraphResult result = execute( """
                UNWIND [1, 1.0, null, 4] AS x
                RETURN stDev(x) AS sample, stDevP(x) AS population,
                       stDev(DISTINCT x) AS distinctSample, stDevP(DISTINCT x) AS distinctPopulation
                """ );
        assertEquals( Math.sqrt( 3 ), cell( result, 0 ).asNumber().doubleValue(), 1e-12 );
        assertEquals( Math.sqrt( 2 ), cell( result, 1 ).asNumber().doubleValue(), 1e-12 );
        assertEquals( Math.sqrt( 4.5 ), cell( result, 2 ).asNumber().doubleValue(), 1e-12 );
        assertEquals( 1.5, cell( result, 3 ).asNumber().doubleValue(), 1e-12 );
        assertEquals( PolyType.DOUBLE, cell( result, 0 ).getType() );

        GraphResult adjacentBigints = execute( """
                UNWIND [9223372036854775806, 9223372036854775807] AS x
                WITH stDevP(x) AS spread
                RETURN spread
                """ );
        assertEquals( 0.5, cell( adjacentBigints, 0 ).asNumber().doubleValue() );
    }


    @Test
    void aggregatesDeviationForEmptyAndSingleValueGroups() {
        GraphResult empty = execute( "MATCH (n:FunctionMissing) RETURN stDev(n.x) AS sample, stDevP(n.x) AS population" );
        assertEquals( 0.0, cell( empty, 0 ).asNumber().doubleValue() );
        assertEquals( 0.0, cell( empty, 1 ).asNumber().doubleValue() );

        GraphResult small = execute( "RETURN stDev(null) AS allNull, stDevP(42) AS singleton" );
        assertEquals( 0.0, cell( small, 0 ).asNumber().doubleValue() );
        assertEquals( 0.0, cell( small, 1 ).asNumber().doubleValue() );

        assertError( "MATCH (n:FunctionMissing) RETURN stDev('x')", "requires a number" );
        assertError( "RETURN stDevP(1, 2)", "requires exactly one argument" );
    }


    private static PolyValue cell( GraphResult result, int column ) {
        assertEquals( 1, result.getData().length );
        String json = result.getData()[0][column];
        return json == null ? null : PolyValue.fromTypedJson( json, PolyValue.class );
    }


    private static List<Long> integers( GraphResult result, int column ) {
        return cell( result, column ).asList().stream().map( v -> v.asNumber().longValue() ).toList();
    }


    private static long integer( String json ) {
        return PolyValue.fromTypedJson( json, PolyValue.class ).asNumber().longValue();
    }


    private static void assertError( String query, String expected ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.contains( expected ), error );
    }

}
