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
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherUnionCaseTest extends CypherTestTemplate {

    @Test
    void unionCombinesIndependentBranchesAndUnionAllKeepsDuplicates() {
        String query = """
                UNWIND [2, 1, 2] AS x RETURN x
                UNION%s
                UNWIND [3, 2] AS y RETURN y AS x
                UNION%s
                RETURN 4 AS x
                """;
        assertEquals( List.of( 1, 2, 3, 4 ), numbers( execute( query.formatted( "", "" ) ) ) );
        assertEquals( List.of( 1, 2, 2, 2, 3, 4 ), numbers( execute( query.formatted( " ALL", " ALL" ) ) ) );
    }


    @Test
    void unionHonorsOrderingAndPaginationWithinEachBranch() {
        GraphResult result = execute( """
                UNWIND [2, 1, 3] AS value RETURN value ORDER BY value SKIP 1 LIMIT 1
                UNION ALL
                UNWIND [8, 3, 7] AS value RETURN value ORDER BY value DESC LIMIT 1
                """ );
        assertEquals( List.of( 2, 8 ), numbers( result ) );
    }


    @Test
    void unionUsesFirstBranchColumnOrderAndPreservesDifferentValueTypes() {
        GraphResult result = execute( """
                RETURN 1 AS a, 'first' AS b UNION ALL
                RETURN 'second' AS b, 'value' AS a
                """ );
        assertEquals( List.of( "a", "b" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        assertEquals( 2, result.getData().length );
        assertEquals( 1, cell( result, 0, 0 ).asNumber().intValue() );
        assertEquals( "first", cell( result, 0, 1 ).asString().value );
        assertEquals( "value", cell( result, 1, 0 ).asString().value );
        assertEquals( "second", cell( result, 1, 1 ).asString().value );
    }


    @Test
    void unionDeduplicatesNullsAndNestedValuesUsingCypherGroupingEquality() {
        GraphResult result = execute( """
                RETURN null AS missing, {list: [1, null], value: 2} AS item
                UNION RETURN null AS missing, {value: 2.0, list: [1.0, null]} AS item
                UNION RETURN null AS missing, {list: [1, null]} AS item
                """ );
        assertEquals( 2, result.getData().length );
        assertNull( cell( result, 0, 0 ) );
        assertNull( cell( result, 1, 0 ) );
        assertEquals( 1, execute( "RETURN 1 AS x UNION RETURN 1.0 AS x" ).getData().length );
    }


    @Test
    void unionReturnsGraphValuesAndDeduplicatesByIdentity() {
        execute( "CREATE (:UnionLeft {name: 'same'}), (:UnionRight {name: 'same'})" );
        GraphResult result = execute( """
                MATCH (left:UnionLeft) RETURN left AS node
                UNION MATCH (right:UnionRight) RETURN right AS node
                UNION MATCH (same:UnionLeft) RETURN same AS node
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( 2, Arrays.stream( result.getData() ).map( row -> value( row[0] ).asNode().getId() ).distinct().count() );
        assertEquals( "same", cell( result, 0, 0 ).asNode().properties.get( PolyString.of( "name" ) ).asString().value );

        GraphResult nested = execute( """
                MATCH (left:UnionLeft) RETURN {nodes: [left]} AS nested
                UNION MATCH (other:UnionRight) RETURN {nodes: [other]} AS nested
                UNION MATCH (again:UnionLeft) RETURN {nodes: [again]} AS nested
                """ );
        assertEquals( 2, nested.getData().length );
    }


    @Test
    void unionCompletesWritesBeforeExecutingSubsequentBranches() {
        GraphResult result = execute( """
                MATCH (before:UnionCreated) RETURN before AS node
                UNION ALL CREATE (created:UnionCreated) RETURN created AS node
                UNION ALL MATCH (after:UnionCreated) RETURN after AS node
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( cell( result, 0, 0 ).asNode().getId(), cell( result, 1, 0 ).asNode().getId() );
    }


    @Test
    void unionRejectsDifferentColumnsAndMixedQualifiers() {
        assertQueryError( "RETURN 1 AS a UNION RETURN 2 AS b", "DifferentColumnsInUnion" );
        assertQueryError( "RETURN 1 UNION RETURN 2", "DifferentColumnsInUnion" );
        assertQueryError( "RETURN 1 AS a UNION ALL RETURN 2 AS a, 3 AS b", "DifferentColumnsInUnion" );
        assertQueryError( "RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a", "InvalidClauseComposition" );
        assertQueryError( "RETURN 1 AS a UNION ALL RETURN 2 AS a UNION RETURN 3 AS a", "InvalidClauseComposition" );
        assertQueryError( "CREATE (:NoReturn) UNION RETURN 2 AS a", "InvalidClauseComposition" );
        assertEquals( List.of( 0 ), numbers( execute( "MATCH (n:NoReturn) RETURN count(n) AS count" ) ) );
    }


    @Test
    void simpleCaseUsesCypherEqualityAndFallsBackOnNull() {
        GraphResult result = execute( """
                RETURN CASE 1.0 WHEN 1 THEN 'numeric' ELSE 'wrong' END AS matching,
                       CASE '1' WHEN 1 THEN 'wrong' ELSE 'string' END AS different,
                       CASE null WHEN null THEN 'wrong' ELSE 'missing' END AS nullSelector,
                       CASE 1 WHEN 2 THEN 'wrong' END AS noMatch
                """ );
        assertEquals( "numeric", cell( result, 0, 0 ).asString().value );
        assertEquals( "string", cell( result, 0, 1 ).asString().value );
        assertEquals( "missing", cell( result, 0, 2 ).asString().value );
        assertNull( cell( result, 0, 3 ) );
    }


    @Test
    void searchedCaseEvaluatesOnlyTheFirstMatchingBranch() {
        GraphResult result = execute( """
                UNWIND [1, 2] AS x
                RETURN x, CASE
                    WHEN null THEN 1 / 0
                    WHEN x = 1 THEN CASE WHEN true THEN 'one' ELSE 1 / 0 END
                    WHEN x = 2 THEN 2
                    WHEN 1 / 0 = 0 THEN 'wrong'
                    ELSE 1 / 0
                END AS value ORDER BY x
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( "one", cell( result, 0, 1 ).asString().value );
        assertEquals( 2, cell( result, 1, 1 ).asNumber().intValue() );
        assertNull( cell( execute( "RETURN CASE WHEN false THEN 1 END AS value" ), 0, 0 ) );
        assertQueryError( "RETURN CASE WHEN 1 THEN 2 END AS value", "predicates require a BOOLEAN" );
    }


    @Test
    void evaluatesExistentialConditionsAndResultsOnlyForReachedCaseBranches() {
        GraphResult result = execute( """
                UNWIND [0, 1, 2] AS n
                RETURN n,
                       CASE WHEN n > 0 THEN EXISTS { WITH n WHERE 1 / n >= 1 RETURN n }
                            ELSE false END AS guarded,
                       CASE WHEN n = 0 THEN true
                            WHEN EXISTS { WITH n WHERE 1 / n >= 1 RETURN n } THEN true
                            WHEN null THEN EXISTS { WITH n WHERE 1 / (n - n) > 0 RETURN n }
                            ELSE false END AS laterCondition,
                       CASE WHEN true THEN true
                            ELSE EXISTS { WITH n WHERE 1 / (n - n) > 0 RETURN n } END AS unusedFallback
                ORDER BY n
                """ );
        assertEquals( 3, result.getData().length );
        assertEquals( List.of( false, true, false ), booleanColumn( result, 1 ) );
        assertEquals( List.of( true, true, false ), booleanColumn( result, 2 ) );
        assertEquals( List.of( true, true, true ), booleanColumn( result, 3 ) );

        assertQueryError( """
                UNWIND [0] AS n
                RETURN CASE WHEN n = 0 THEN EXISTS { WITH n WHERE 1 / n > 0 RETURN n }
                            ELSE false END AS selected
                """, "Division by zero" );
    }


    @Test
    void preservesSimpleCaseEqualityAndSkipsExistentialResults() {
        GraphResult result = execute( """
                UNWIND [0, 1] AS n
                RETURN n,
                       CASE n WHEN 0 THEN true
                              WHEN 1 THEN EXISTS { WITH n WHERE n = 1 RETURN n }
                              WHEN EXISTS { WITH n WHERE 1 / (n - n) > 0 RETURN n } THEN false
                              ELSE false END AS selected,
                       CASE null WHEN null THEN EXISTS { WITH n WHERE 1 / (n - n) > 0 RETURN n }
                                 ELSE false END AS nullSelector,
                       CASE n WHEN 1 THEN EXISTS { WITH n WHERE n = 1 RETURN n } END AS noFallback
                ORDER BY n
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( List.of( true, true ), booleanColumn( result, 1 ) );
        assertEquals( List.of( false, false ), booleanColumn( result, 2 ) );
        assertNull( cell( result, 0, 3 ) );
        assertEquals( true, cell( result, 1, 3 ).asBoolean().value );
    }


    @Test
    void keepsExistentialCaseBranchesLazyInAndAroundListComprehensions() {
        GraphResult result = execute( """
                UNWIND [0, 1] AS n
                RETURN n,
                       CASE WHEN n = 0 THEN []
                            ELSE [x IN [n] WHERE EXISTS { WITH x WHERE 1 / x > 0 RETURN x }] END AS kept,
                       [x IN [n] | CASE WHEN x = 0 THEN false
                                       ELSE EXISTS { WITH x WHERE 1 / x > 0 RETURN x } END] AS elements
                ORDER BY n
                """ );
        assertEquals( 2, result.getData().length );
        assertTrue( cell( result, 0, 1 ).asList().isEmpty() );
        assertEquals( 1, cell( result, 1, 1 ).asList().get( 0 ).asNumber().intValue() );
        assertEquals( false, cell( result, 0, 2 ).asList().get( 0 ).asBoolean().value );
        assertEquals( true, cell( result, 1, 2 ).asList().get( 0 ).asBoolean().value );
    }


    @Test
    void casePreservesListValuesAndWorksInPredicates() {
        GraphResult result = execute( """
                UNWIND [true, false] AS flag
                WITH flag, CASE WHEN flag THEN [1] ELSE ['text'] END AS items
                WHERE CASE WHEN flag THEN null ELSE true END
                RETURN items[0] AS element
                """ );
        assertEquals( 1, result.getData().length );
        assertEquals( "text", cell( result, 0, 0 ).asString().value );
    }


    private static List<Integer> numbers( GraphResult result ) {
        return Arrays.stream( result.getData() ).map( row -> value( row[0] ).asNumber().intValue() ).sorted().toList();
    }


    private static List<Boolean> booleanColumn( GraphResult result, int column ) {
        return Arrays.stream( result.getData() ).map( row -> value( row[column] ).asBoolean().value ).toList();
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        return value( result.getData()[row][column] );
    }


    private static PolyValue value( String json ) {
        PolyValue value = json == null ? null : PolyValue.fromTypedJson( json, PolyValue.class );
        return value == null || value.isNull() ? null : value;
    }


    private static void assertQueryError( String query, String expected ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.contains( expected ), error );
    }

}
