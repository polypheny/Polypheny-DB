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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherFunctionsTest extends CypherTestTemplate {

    @Test
    void slicesStringsByUnicodeCodePoint() {
        GraphResult result = execute( """
                WITH 'a😀b' AS text
                RETURN left(text, 2) AS prefix, right(text, 2) AS suffix,
                       substring(text, 1, 1) AS middle, substring(text, 2) AS tail,
                       substring(text, 9223372036854775807) AS pastEnd,
                       substring(text, 1, 9223372036854775807) AS longLength,
                       substring(text, 1, 0) AS zeroLength, right(text, 9223372036854775807) AS whole
                """ );
        assertEquals( "a😀", cell( result, 0 ).asString().value );
        assertEquals( "😀b", cell( result, 1 ).asString().value );
        assertEquals( "😀", cell( result, 2 ).asString().value );
        assertEquals( "b", cell( result, 3 ).asString().value );
        assertEquals( "", cell( result, 4 ).asString().value );
        assertEquals( "😀b", cell( result, 5 ).asString().value );
        assertEquals( "", cell( result, 6 ).asString().value );
        assertEquals( "a😀b", cell( result, 7 ).asString().value );
    }


    @Test
    void trimsAndChangesStringCase() {
        GraphResult result = execute( """
                WITH '  Cypher  ' AS value
                RETURN ltrim(value) AS leading, rtrim(value) AS trailing, trim(value) AS trimmed,
                       toLower(trim(value)) AS lowered, toUpper('groß') AS upperCase,
                       lower('POLYPHENY') AS lowerAlias, upper('polypheny') AS upperAlias,
                       toLower(null) AS missing, trim('   ') AS blank
                """ );
        assertEquals( "Cypher  ", cell( result, 0 ).asString().value );
        assertEquals( "  Cypher", cell( result, 1 ).asString().value );
        assertEquals( "Cypher", cell( result, 2 ).asString().value );
        assertEquals( "cypher", cell( result, 3 ).asString().value );
        assertEquals( "GROSS", cell( result, 4 ).asString().value );
        assertEquals( "polypheny", cell( result, 5 ).asString().value );
        assertEquals( "POLYPHENY", cell( result, 6 ).asString().value );
        assertNull( cell( result, 7 ) );
        assertEquals( "", cell( result, 8 ).asString().value );
    }


    @Test
    void splitsAndReplacesLiteralText() {
        GraphResult result = execute( """
                RETURN replace('a.$.b.$', '.$', '_') AS replaced, replace('😀', '', '-') AS boundaries,
                       split('.a..', '.') AS parts, split('a😀', '') AS points, split('', 'x') AS empty,
                       split('a', null) AS missingDelimiter, replace('a', 'a', null) AS missingReplacement
                """ );
        assertEquals( "a_.b_", cell( result, 0 ).asString().value );
        assertEquals( "-😀-", cell( result, 1 ).asString().value );
        assertEquals( List.of( "", "a", "", "" ), strings( result, 2 ) );
        assertEquals( List.of( "a", "😀" ), strings( result, 3 ) );
        assertEquals( List.of( "" ), strings( result, 4 ) );
        assertNull( cell( result, 5 ) );
        assertNull( cell( result, 6 ) );

        GraphResult unwind = execute( "UNWIND split('one1two', '1') AS word RETURN count(word) AS count" );
        assertEquals( 2L, cell( unwind, 0 ).asNumber().longValue() );
    }


    @Test
    void reversesStringsAndListsAndConvertsScalarValuesToString() {
        GraphResult result = execute( """
                WITH reverse([1,'a',null]) AS values
                RETURN reverse('a😀b') AS text, values[0] IS NULL AS missingElement, values[1] AS textElement,
                       values[2] AS numberElement, reverse([]) AS empty, reverse(null) AS missing,
                       toString(9007199254740993) AS integer, toString(8.0) AS floating,
                       toString(true) AS boolean, toString('text') AS same, toString(null) AS absent
                """ );
        assertEquals( "b😀a", cell( result, 0 ).asString().value );
        assertTrue( cell( result, 1 ).asBoolean().value );
        assertEquals( "a", cell( result, 2 ).asString().value );
        assertEquals( 1L, cell( result, 3 ).asNumber().longValue() );
        assertTrue( cell( result, 4 ).asList().isEmpty() );
        assertNull( cell( result, 5 ) );
        assertEquals( "9007199254740993", cell( result, 6 ).asString().value );
        assertEquals( "8.0", cell( result, 7 ).asString().value );
        assertEquals( "true", cell( result, 8 ).asString().value );
        assertEquals( "text", cell( result, 9 ).asString().value );
        assertNull( cell( result, 10 ) );
    }


    @Test
    void reportsWrongStringFunctionArityAndTypes() {
        assertError( "RETURN substring('abc')", "substring() requires 2 or 3 arguments" );
        assertError( "RETURN replace('a', 'a')", "replace() requires exactly 3 arguments" );
        assertError( "RETURN substring('abc', null)", "requires a non-negative integer start" );
        assertError( "RETURN substring('abc', 0, null)", "requires a non-negative integer length" );
        assertError( "RETURN substring('abc', -1)", "requires a non-negative integer start" );
        assertError( "RETURN substring('abc', 0, 1.5)", "requires a non-negative integer length" );
        assertError( "RETURN left('abc', null)", "requires a positive integer length" );
        assertError( "RETURN right('abc', -1)", "requires a positive integer length" );
        assertError( "UNWIND ['abc', 4] AS value RETURN toUpper(value)", "toUpper() requires a string for argument 1" );
        assertError( "RETURN reverse(4)", "reverse() requires a string or list" );
        assertError( "RETURN split('a', 4)", "split() requires a string for argument 2" );
        assertError( "RETURN toString({k: 1})", "toString() requires a string, number, boolean, or temporal value" );

        GraphResult result = execute( "RETURN left(null, null) AS a, substring(null, -1) AS b, split(null, '\\u002e') AS c" );
        assertNull( cell( result, 0 ) );
        assertNull( cell( result, 1 ) );
        assertNull( cell( result, 2 ) );
    }


    @Test
    void calculatesAbsoluteValuesAndIntegerSignsWithoutLosingPrecision() {
        GraphResult result = execute( """
                RETURN abs(-9007199254740993) AS exact, abs(-1.5) AS floating,
                       sign(-2.75) AS negative, sign(-0.0) AS zero, sign(42) AS positive,
                       abs(null) AS missing, sign(null) AS missingSign
                """ );
        assertEquals( PolyType.BIGINT, cell( result, 0 ).getType() );
        assertEquals( 9007199254740993L, cell( result, 0 ).asNumber().longValue() );
        assertEquals( PolyType.DOUBLE, cell( result, 1 ).getType() );
        assertEquals( 1.5, cell( result, 1 ).asNumber().doubleValue() );
        for ( int i = 0; i < 3; i++ ) {
            assertTrue( PolyType.INT_TYPES.contains( cell( result, i + 2 ).getType() ) );
            assertEquals( i - 1L, cell( result, i + 2 ).asNumber().longValue() );
        }
        assertNull( cell( result, 5 ) );
        assertNull( cell( result, 6 ) );
        assertError( "RETURN abs(-9223372036854775808)", "abs() integer result is out of range" );
        assertError( "UNWIND [1,'a'] AS value RETURN abs(value)", "abs() requires a number for argument 1" );
    }


    @Test
    void roundsToFloatingValuesAndHandlesTiesAndLargeArguments() {
        GraphResult result = execute( """
                RETURN ceil(0.1) AS up, ceil(-0.1) AS negativeUp, floor(0.9) AS down, floor(-0.1) AS negativeDown,
                       round(3.14) AS nearest, round(-1.5) AS negativeTie, round(1.5) AS positiveTie,
                       round(0.49999999999999994) AS belowTie, round(1.0e30) AS beyondLong
                """ );
        assertFloats( result, 1.0, 0.0, 0.0, -1.0, 3.0, -1.0, 2.0, 0.0, 1.0e30 );
    }


    @Test
    void evaluatesMathematicalConstantsLogarithmsAndTrigonometry() {
        GraphResult logarithmic = execute( """
                RETURN e() AS naturalBase, pi() AS circle, exp(1) AS exponential,
                       log(e()) AS naturalLog, log10(1000) AS commonLog, sqrt(12.96) AS root
                """ );
        assertFloats( logarithmic, Math.E, Math.PI, Math.E, 1.0, 3.0, 3.6 );

        GraphResult circular = execute( """
                RETURN acos(0) AS arccosine, asin(1) AS arcsine, atan(1) AS arctangent,
                       atan2(1, -1) AS quadrant, cos(pi()) AS cosine, cot(pi() / 4) AS cotangent,
                       degrees(pi()) AS degrees, radians(180) AS radians, sin(pi() / 2) AS sine, tan(pi() / 4) AS tangent
                """ );
        assertFloats( circular, Math.PI / 2, Math.PI / 2, Math.PI / 4, 3 * Math.PI / 4, -1.0, 1.0, 180.0, Math.PI, 1.0, 1.0 );
    }


    @Test
    void handlesNumericDomainsNullsAndArgumentErrors() {
        GraphResult result = execute( """
                RETURN sqrt(-1) AS negativeRoot, log(0) AS zeroLog, log10(0) AS zeroLog10,
                       acos(2) AS badArccosine, asin(-2) AS badArcsine, cot(0) AS zeroCotangent,
                       atan2(1, null) AS missingCoordinate, ceil(null) AS missingCeil,
                       floor(null) AS missingFloor, round(null) AS missingRound,
                       exp(null) AS missingExp, degrees(null) AS missingDegrees
                """ );
        for ( int i = 0; i < 12; i++ ) {
            assertNull( cell( result, i ) );
        }
        assertError( "RETURN atan2(1, 'x')", "atan2() requires a number for argument 2" );
        assertError( "RETURN cos(true)", "cos() requires a number for argument 1" );
        assertError( "RETURN sqrt()", "sqrt() requires exactly 1 argument" );
        assertError( "RETURN pi(1)", "pi() requires exactly 0 arguments" );
        assertError( "RETURN rand(1)", "rand() requires exactly 0 arguments" );
    }


    @Test
    void convertsNumbersAndStringsAndUsesConstantFunctionsInRowBounds() {
        GraphResult integers = execute( """
                WITH '1.7' AS input
                RETURN toInteger(82.9) AS positive, toInteger(-82.9) AS negative, toInteger(input) AS decimalText,
                       toInteger('9223372036854775807.9') AS maxInteger, toInteger(-9223372036854775808.0) AS minInteger,
                       toInteger(9007199254740993) AS exact, toInteger('9223372036854775808') AS outOfRange,
                       toInteger('1e2147483647') AS hugeExponent, toInteger('bad') AS invalid,
                       toInteger('') AS empty, toInteger(null) AS missing
                """ );
        long[] values = { 82L, -82L, 1L, Long.MAX_VALUE, Long.MIN_VALUE, 9007199254740993L };
        for ( int i = 0; i < values.length; i++ ) {
            assertEquals( PolyType.BIGINT, cell( integers, i ).getType() );
            assertEquals( values[i], cell( integers, i ).asNumber().longValue() );
        }
        for ( int i = values.length; i < 11; i++ ) {
            assertNull( cell( integers, i ) );
        }

        GraphResult floating = execute( "RETURN toFloat(2) AS whole, toFloat(2.5) AS float, toFloat('-2.9e0') AS text" );
        assertFloats( floating, 2.0, 2.5, -2.9 );
        GraphResult invalid = execute( "RETURN toFloat('bad') AS invalid, toFloat(null) AS missing" );
        assertNull( cell( invalid, 0 ) );
        assertNull( cell( invalid, 1 ) );
        assertError( "RETURN toInteger({a:1})", "toInteger() requires a string or number" );
        assertError( "RETURN toFloat(false)", "toFloat() requires a string or number" );

        GraphResult limited = execute( "UNWIND [3, 1, 2] AS n WITH n ORDER BY n LIMIT toInteger(ceil(1.7)) RETURN n" );
        assertEquals( 2, limited.getData().length );
        assertEquals( 1L, integer( limited.getData()[0][0] ) );
        assertEquals( 2L, integer( limited.getData()[1][0] ) );
        assertEquals( 1, execute( "UNWIND [1, 2, 3] AS n RETURN n SKIP toInteger(2 + 0 * rand())" ).getData().length );
    }


    @Test
    void convertsBooleansAndCaseInsensitiveBooleanStrings() {
        GraphResult result = execute( """
                RETURN toBoolean(false) AS existing, toBoolean('TRUE') AS parsedTrue,
                       toBoolean('FaLsE') AS parsedFalse, toBoolean('wrong') AS invalid,
                       toBoolean(' true ') AS padded, toBoolean(null) AS missing
                """ );
        assertEquals( false, cell( result, 0 ).asBoolean().value );
        assertEquals( true, cell( result, 1 ).asBoolean().value );
        assertEquals( false, cell( result, 2 ).asBoolean().value );
        assertNull( cell( result, 3 ) );
        assertNull( cell( result, 4 ) );
        assertNull( cell( result, 5 ) );
        assertError( "RETURN [x IN [true, 1.0] | toBoolean(x)]", "InvalidArgumentValue: toBoolean() requires a string or boolean" );
    }


    @Test
    void generatesIndependentRandomValuesAcrossRowsAndCallSites() {
        GraphResult result = execute( "UNWIND range(1, 24) AS x RETURN rand() AS first, rand() AS second" );
        assertEquals( 24, result.getData().length );
        Set<Double> first = new HashSet<>();
        Set<Double> second = new HashSet<>();
        boolean differentWithinRow = false;
        for ( String[] row : result.getData() ) {
            PolyValue left = PolyValue.fromTypedJson( row[0], PolyValue.class );
            PolyValue right = PolyValue.fromTypedJson( row[1], PolyValue.class );
            assertEquals( PolyType.DOUBLE, left.getType() );
            assertEquals( PolyType.DOUBLE, right.getType() );
            double a = left.asNumber().doubleValue();
            double b = right.asNumber().doubleValue();
            assertTrue( a >= 0 && a < 1 && b >= 0 && b < 1 );
            first.add( a );
            second.add( b );
            differentWithinRow |= a != b;
        }
        assertTrue( first.size() > 1 && second.size() > 1 );
        assertTrue( differentWithinRow );

        GraphResult same = execute( "WITH rand() AS value RETURN value AS first, value AS second" );
        assertEquals( cell( same, 0 ), cell( same, 1 ) );
    }


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
                RETURN range(-1, 1) AS `ascending`, range(4, -1, -2) AS `descending`,
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
    void computesDiscreteAndContinuousPercentilesPerGroup() {
        GraphResult result = execute( """
                UNWIND [0.25, 0.5] AS p
                UNWIND [4, 1, 2, 3, null] AS n
                RETURN p, percentileDisc(n, p) AS discrete, percentileCont(n, p) AS continuous
                ORDER BY p
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( 1, integer( result.getData()[0][1] ) );
        assertEquals( 1.75, PolyValue.fromTypedJson( result.getData()[0][2], PolyValue.class ).asNumber().doubleValue() );
        assertEquals( 2, integer( result.getData()[1][1] ) );
        assertEquals( 2.5, PolyValue.fromTypedJson( result.getData()[1][2], PolyValue.class ).asNumber().doubleValue() );

        GraphResult distinct = execute( "UNWIND [1, 1, 2, 4] AS n RETURN percentileDisc(DISTINCT n, 0.5) AS p" );
        assertEquals( 2, cell( distinct, 0 ).asNumber().longValue() );
        GraphResult empty = execute( "UNWIND [] AS n RETURN percentileCont(n, 0.5) AS p" );
        assertNull( cell( empty, 0 ) );
        assertError( "UNWIND [1] AS n RETURN percentileDisc(n, n + 1)", "NumberOutOfRange" );
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


    private static List<String> strings( GraphResult result, int column ) {
        return cell( result, column ).asList().stream().map( value -> value.asString().value ).toList();
    }


    private static long integer( String json ) {
        return PolyValue.fromTypedJson( json, PolyValue.class ).asNumber().longValue();
    }


    private static void assertFloats( GraphResult result, double... expected ) {
        assertEquals( expected.length, result.getData()[0].length );
        for ( int i = 0; i < expected.length; i++ ) {
            PolyValue value = cell( result, i );
            assertEquals( PolyType.DOUBLE, value.getType() );
            assertEquals( expected[i], value.asNumber().doubleValue(), 1.0e-12 );
        }
    }


    private static void assertError( String query, String expected ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.contains( expected ), error );
    }

}
