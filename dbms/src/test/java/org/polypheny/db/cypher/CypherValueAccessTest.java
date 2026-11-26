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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherValueAccessTest extends CypherTestTemplate {

    @Test
    void indexesAndSlicesListsWithCypherBounds() {
        GraphResult result = execute( """
                WITH [1,2,3] AS xs
                RETURN xs[-1] AS tail, [[9]][0][0] AS nested, xs[-3..-1] AS slice,
                       xs[..1] AS prefix, xs[1..] AS suffix, xs[-9223372036854775808..9223372036854775807] AS full,
                       xs[2..1] AS empty, xs[9223372036854775807] AS outOfBounds,
                       xs[null] AS nullIndex, xs[..null] AS nullBound, null[..] AS nullList
                """ );
        assertEquals( 3, number( result, 0 ) );
        assertEquals( 9, number( result, 1 ) );
        assertEquals( List.of( 1L, 2L ), numbers( result, 2 ) );
        assertEquals( List.of( 1L ), numbers( result, 3 ) );
        assertEquals( List.of( 2L, 3L ), numbers( result, 4 ) );
        assertEquals( List.of( 1L, 2L, 3L ), numbers( result, 5 ) );
        assertEquals( List.of(), numbers( result, 6 ) );
        for ( int column = 7; column < 11; column++ ) {
            assertNull( cell( result, column ) );
        }

        assertError( "RETURN [1]['0'] AS result", "must be an integer" );
        assertError( "RETURN [1][0.0..] AS result", "must be an integer" );
        assertError( "RETURN 'ab'[0] AS result", "Indexing requires" );
        assertError( "RETURN {x:1}[1] AS result", "must be a string" );
    }


    @Test
    void constructsMapsFromExpressionsAndReadsTheirFields() {
        GraphResult result = execute( """
                WITH 7 AS x, 'key' AS k
                WITH {key:x, nested:{key:x+1}, present:null} AS m, k
                RETURN m.key AS staticValue, m[k] AS dynamicValue, m.nested[k] AS nestedValue,
                       m.present AS nullValue, m.absent AS missingValue, {value:9}.value AS literalAccess
                """ );
        assertEquals( 7, number( result, 0 ) );
        assertEquals( 7, number( result, 1 ) );
        assertEquals( 8, number( result, 2 ) );
        assertNull( cell( result, 3 ) );
        assertNull( cell( result, 4 ) );
        assertEquals( 9, number( result, 5 ) );
    }


    @Test
    void exposesNodesRelationshipsAndLengthInTraversalOrder() {
        execute( "CREATE (:PathSource {name:'start'})-[:ACCESS {weight:5}]->()-[:ACCESS {weight:7}]->(:PathEnd {name:'end'})" );
        GraphResult result = execute( """
                MATCH p=(:PathSource)-[rs:ACCESS*]->(:PathEnd)
                RETURN nodes(p)[0].name AS first, nodes(p)[-1].name AS lastNode,
                       relationships(p)[0].weight AS firstWeight, last(rs[1..]).weight AS lastWeight,
                       length(p) AS length
                """ );
        assertEquals( "start", cell( result, 0 ).asString().value );
        assertEquals( "end", cell( result, 1 ).asString().value );
        assertEquals( 5, number( result, 2 ) );
        assertEquals( 7, number( result, 3 ) );
        assertEquals( 2, number( result, 4 ) );

        GraphResult noPath = execute( "OPTIONAL MATCH p=(:Missing)-->() RETURN nodes(p) AS n, relationships(p) AS r, length(p) AS l" );
        assertNull( cell( noPath, 0 ) );
        assertNull( cell( noPath, 1 ) );
        assertNull( cell( noPath, 2 ) );
        assertError( "MATCH (n:Missing) RETURN length(n) AS size", "requires a path" );
    }


    @Test
    void collectsNamedPathValuesWithoutIncludingNulls() {
        execute( "CREATE (:CollectStart)-[:COLLECTED]->({name:'first'})-[:COLLECTED]->({name:'second'})" );
        GraphResult paths = execute( """
                MATCH p=(:CollectStart)-[:COLLECTED*]->()
                WITH collect(nodes(p)) AS paths, length(p) AS hops
                RETURN last(last(paths)).name AS tail, hops ORDER BY hops
                """ );
        assertEquals( 2, paths.getData().length );
        assertEquals( "first", cell( paths, 0, 0 ).asString().value );
        assertEquals( 1, cell( paths, 0, 1 ).asNumber().longValue() );
        assertEquals( "second", cell( paths, 1, 0 ).asString().value );
        assertEquals( 2, cell( paths, 1, 1 ).asNumber().longValue() );

        GraphResult missing = execute( "OPTIONAL MATCH p=(:CollectMissing)-->() WITH collect(p) AS paths RETURN paths, last(paths) AS tail" );
        assertEquals( List.of(), cell( missing, 0 ).asList() );
        assertNull( cell( missing, 1 ) );
    }


    @Test
    void collectsDistinctValuesUsingCypherGroupingEquality() {
        GraphResult numbers = execute( """
                UNWIND [1,1.0,null,2,1] AS value
                RETURN collect(DISTINCT value) AS values
                """ );
        assertEquals( List.of( 1L, 2L ), numbers( numbers, 0 ).stream().sorted().toList() );

        GraphResult nested = execute( """
                UNWIND [{x:[1,null]}, {x:[1.0,null]}, {x:[2,null]}] AS value
                WITH collect(DISTINCT value) AS collected
                UNWIND collected AS v
                RETURN v.x[0] AS value ORDER BY value
                """ );
        assertEquals( 2, nested.getData().length );
        assertEquals( 1, cell( nested, 0, 0 ).asNumber().longValue() );
        assertEquals( 2, cell( nested, 1, 0 ).asNumber().longValue() );
    }


    @Test
    void countsGroupedRowsAndDistinctGroupingValuesIndependently() {
        GraphResult result = execute( """
                UNWIND [2, null, 1, 1, null] AS n
                RETURN n, count(*) AS rows, count(DISTINCT n) AS distinctValues ORDER BY n
                """ );
        assertEquals( 3, result.getData().length );
        assertEquals( 1, cell( result, 0, 0 ).asNumber().longValue() );
        assertEquals( 2, cell( result, 0, 1 ).asNumber().longValue() );
        assertEquals( 1, cell( result, 0, 2 ).asNumber().longValue() );
        assertEquals( 2, cell( result, 1, 0 ).asNumber().longValue() );
        assertEquals( 1, cell( result, 1, 1 ).asNumber().longValue() );
        assertEquals( 1, cell( result, 1, 2 ).asNumber().longValue() );
        assertNull( cell( result, 2, 0 ) );
        assertEquals( 2, cell( result, 2, 1 ).asNumber().longValue() );
        assertEquals( 0, cell( result, 2, 2 ).asNumber().longValue() );
    }


    @Test
    void namesUnaliasedScalarColumnsFromTheirSourceAndRejectsDistinct() {
        GraphResult result = execute( "RETURN last([1,2]),\n\tlast([3, 4]), [[1]][0][0]" );
        assertEquals( List.of( "last([1,2])", "last([3, 4])", "[[1]][0][0]" ),
                Arrays.stream( result.getHeader() ).map( header -> header.name ).toList() );
        assertEquals( 2, number( result, 0 ) );
        assertEquals( 4, number( result, 1 ) );
        assertEquals( 1, number( result, 2 ) );
        assertError( "RETURN last(DISTINCT [1,2]) AS result", "DISTINCT requires an aggregating function" );
    }


    @Test
    void preservesUnicodeEscapesInUnaliasedColumnNames() {
        String expression = "n + \\u006e";
        GraphResult result = execute( "WITH 1 AS n\nRETURN " + expression );
        assertEquals( expression, result.getHeader()[0].name );
        assertEquals( 2, number( result, 0 ) );
    }


    @Test
    void usesSigned64BitArithmeticAndCypherComparison() {
        GraphResult result = execute( """
                RETURN 4611686018427387905 + 2 AS large, -9223372036854775808 AS lower,
                       0x10 + 0o10 AS bases, '1' < 1 AS incomparable, 0.0 / 0.0 = 0.0 / 0.0 AS equalNan,
                       [1]+[2] AS concatenated
                """ );
        assertEquals( 4611686018427387907L, number( result, 0 ) );
        assertEquals( Long.MIN_VALUE, number( result, 1 ) );
        assertEquals( 24, number( result, 2 ) );
        assertNull( cell( result, 3 ) );
        assertEquals( false, cell( result, 4 ).asBoolean().value );
        assertEquals( List.of( 1L, 2L ), numbers( result, 5 ) );
    }


    @Test
    void comparesAdjacentOperandsInChains() {
        GraphResult result = execute( """
                RETURN 1 < 2 <= 2 <> 3 AS allTrue, 'a' < 'c' < 'b' AS falseComparison,
                       1 < null < 3 AS unknownComparison, 1 < null < 3 < 2 AS falseWithNull
                """ );
        assertEquals( true, cell( result, 0 ).asBoolean().value );
        assertEquals( false, cell( result, 1 ).asBoolean().value );
        assertNull( cell( result, 2 ) );
        assertEquals( false, cell( result, 3 ).asBoolean().value );
    }


    @Test
    void distinguishesMalformedLiteralsFromNumericOverflow() {
        assertError( "RETURN 0x1j", "InvalidNumberLiteral" );
        assertError( "RETURN -0x8000000000000001", "IntegerOverflow" );
        assertError( "RETURN 1.34E999", "FloatingPointOverflow" );
        assertError( "RETURN '" + "\\" + "uH'", "InvalidUnicodeLiteral" );
        assertError( "RETURN 42 — 41", "InvalidUnicodeCharacter" );
    }


    @Test
    void validatesAggregateOperandsAndGroupingKeys() {
        assertError( "RETURN count(count(*))", "NestedAggregation" );
        assertError( "RETURN count(rand())", "NonConstantExpression" );
        assertError( "UNWIND [1] AS n RETURN n + 1, n + 1 + count(*)", "AmbiguousAggregationExpression" );
        assertError( "UNWIND [1] AS n WITH n + 1 AS key, n + 1 + count(*) AS total RETURN total", "AmbiguousAggregationExpression" );
        assertEquals( 2, number( execute( "UNWIND [1, 2] AS n WITH rand() AS r RETURN count(r)" ), 0 ) );
        assertEquals( 1, number( execute( "RETURN count(date('2020-01-01'))" ), 0 ) );
    }


    @Test
    void appliesSigned64BitSkipAndLimitWithAndWithoutOrdering() {
        for ( String bound : List.of( "2147483648", "4294967296", "9223372036854775807" ) ) {
            assertEquals( 1, number( execute( "RETURN 1 AS n LIMIT " + bound ), 0 ) );
            assertEquals( 0, execute( "RETURN 1 AS n SKIP " + bound ).getData().length );
            assertEquals( 0, execute( "UNWIND [1, 3, 2] AS n RETURN n ORDER BY n DESC SKIP " + bound + " LIMIT 1" ).getData().length );

            GraphResult ordered = execute( "UNWIND [1, 3, 2] AS n WITH n ORDER BY n DESC SKIP 1 LIMIT " + bound + " RETURN n" );
            assertEquals( 2, ordered.getData().length );
            assertEquals( 2, cell( ordered, 0, 0 ).asNumber().longValue() );
            assertEquals( 1, cell( ordered, 1, 0 ).asNumber().longValue() );
        }
    }


    @Test
    void retainsIndividualElementTypesWhenConcatenatingHeterogeneousLists() {
        GraphResult result = execute( "RETURN ([1]+2.0)[0] AS i, ([1]+[2.0])[1] AS f, ([] + [1])[0] AS e" );
        assertEquals( "ANY", result.getHeader()[0].dataType );
        assertEquals( "ANY", result.getHeader()[1].dataType );
        assertEquals( "INTEGER", result.getHeader()[2].dataType );
        assertEquals( PolyType.INTEGER, cell( result, 0 ).getType() );
        assertEquals( PolyType.DOUBLE, cell( result, 1 ).getType() );
        assertEquals( 1, number( result, 0 ) );
        assertEquals( 2.0, cell( result, 1 ).asNumber().doubleValue() );
        assertEquals( 1, number( result, 2 ) );
    }


    private static void assertError( String query, String message ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error );
        assertTrue( error.contains( message ), error );
    }


    private static List<Long> numbers( GraphResult result, int column ) {
        return cell( result, column ).asList().stream().map( value -> value.asNumber().longValue() ).toList();
    }


    private static long number( GraphResult result, int column ) {
        return cell( result, column ).asNumber().longValue();
    }


    private static PolyValue cell( GraphResult result, int column ) {
        return cell( result, 0, column );
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        String json = result.getData()[row][column];
        return json == null ? null : PolyValue.fromTypedJson( json, PolyValue.class );
    }

}
