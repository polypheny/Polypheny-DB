/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.mql.mql2alg.dql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.mql.mql.MqlTest;
import org.polypheny.db.mql.mql2alg.Mql2AlgTest;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;

@Ignore // this test is an extreme time sink, use integration tests
public class Mql2AlgFindTest extends Mql2AlgTest {


    public String find( String match ) {
        return "db.secrets.find({" + match + "})";
    }


    public String find( String match, String project ) {
        return "db.secrets.find({" + match + "},{" + project + "})";
    }


    @Test
    public void testEmptyMatch() {
        AlgRoot root = translate( find( "" ) );
        assertTrue( root.alg instanceof DocumentScan );
    }


    @Test
    public void testSingleMatch() {
        AlgRoot root = translate( find( "\"_id\":\"value\"" ) );
        RexCall condition = getConditionTestFilter( root );
        assertEquals( OperatorName.MQL_EQUALS, condition.op.getOperatorName() );

        assertTrue( condition.operands.get( 0 ).isA( Kind.INPUT_REF ) );
        assertEquals( 0, ((RexInputRef) condition.operands.get( 0 )).getIndex() );

        testCastLiteral( condition.operands.get( 1 ), "value", String.class );
    }


    @Test
    public void testSingleMatchDocument() {
        AlgRoot root = translate( find( "\"key\":\"value\"" ) );
        // test general structure
        RexCall condition = getConditionTestFilter( root );

        testJsonValueCond( condition, "key", "value", Kind.EQUALS );
    }


    @Test
    public void testMultipleMatchDocument() {
        AlgRoot root = translate( find( "\"key\":\"value\",\"key1\":\"value1\"" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( Kind.AND, condition.op.getKind() );

        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", Kind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1", "value1", Kind.EQUALS );
    }


    @Test
    public void testSingleNestedDocument() {
        AlgRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( Kind.EQUALS, condition.op.getKind() );

        testJsonValueCond( condition, "key.key1", "value1", Kind.EQUALS );

        testCastLiteral( condition.operands.get( 1 ), "value1", String.class );
    }


    @Test
    public void testMultipleNestedDocument() {
        AlgRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}, \"key1\":{\"key2\":\"value2\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( Kind.AND, condition.op.getKind() );

        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key.key1", "value1", Kind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1.key2", "value2", Kind.EQUALS );
    }


    @Test
    public void testSingleExists() {
        AlgRoot root = translate( find( "\"key\": { \"$exists\": true}" ) );

        RexCall condition = getConditionTestFilter( root );

        testJsonExists( condition, "key" );
    }


    @Test
    public void testSingleNegativeExists() {
        AlgRoot root = translate( find( "\"key\": { \"$exists\": false}" ) );

        RexCall condition = getConditionTestFilter( root );

        testNot( condition );

        RexCall json = assertRexCall( condition, 0 );
        testJsonExists( json, "key" );
    }


    @Test
    public void testMultipleExists() {
        AlgRoot root = translate( find( "\"key\": { \"$exists\": true}, \"key1\":{ \"$exists\": false}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( Kind.AND, condition.op.getKind() );
        assertEquals( 2, condition.operands.size() );
        RexCall left = assertRexCall( condition, 0 );
        testJsonExists( left, "key" );

        RexCall right = assertRexCall( condition, 1 );
        testNot( right );
        RexCall rightJson = assertRexCall( right, 0 );
        testJsonExists( rightJson, "key1" );
    }


    @Test
    public void testSingleEquals() {
        AlgRoot root = translate( find( "\"key\": { \"$eq\": \"value\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        testJsonValueCond( condition, "key", "value", Kind.EQUALS );
    }


    @Test
    public void testBiComparisons() {
        // some make no sense, maybe fix in the future TODO DL
        for ( Entry<String, Operator> entry : MqlTest.getBiComparisons().entrySet() ) {
            AlgRoot root = translate( find( "\"key\": { \"" + entry.getKey() + "\": \"value\"}" ) );

            RexCall condition = getConditionTestFilter( root );

            testJsonValueCond( condition, "key", "value", entry.getValue().getKind() );
        }
    }


    @Test
    public void testInOperator() {
        AlgRoot root = translate( find( "\"key\": { \"$in\": [\"value\",\"value1\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( Kind.OR, condition.op.getKind() );
        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", Kind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", Kind.EQUALS );
    }


    @Test
    public void testNinOperator() {
        AlgRoot root = translate( find( "\"key\": { \"$nin\": [\"value\",\"value1\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( Kind.AND, condition.op.getKind() );
        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", Kind.NOT_EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", Kind.NOT_EQUALS );
    }


    @Test
    public void testLogicalAndOperator() {
        AlgRoot root = translate( find( "\"key\": { \"$and\": [{\"$eq\": \"value\"},{\"$gt\": \"value1\"}]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( 2, condition.operands.size() );
        assertEquals( Kind.AND, condition.op.getKind() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", Kind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", Kind.GREATER_THAN );

        // less deep

        root = translate( find( " \"$and\": [{\"key\":\"value\"},{\"key1\":\"value1\"}]" ) );

        condition = getConditionTestFilter( root );

        assertEquals( 2, condition.operands.size() );
        assertEquals( Kind.AND, condition.op.getKind() );

        left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", Kind.EQUALS );

        right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1", "value1", Kind.EQUALS );
    }


    @Test
    public void testMixedNestedAndLogical() {
        AlgRoot root = translate( find( "\"key\": {\"$gt\": 3, \"$and\": [{\"$not\":{ \"sub\": 4}},{\"$lt\": 15}]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( 3, condition.operands.size() );

        RexCall gt = assertRexCall( condition, 0 );
        RexCall not = assertRexCall( condition, 1 );
        RexCall lt = assertRexCall( condition, 2 );

        testJsonValueCond( gt, "key", 3, Kind.GREATER_THAN );
        assertEquals( Kind.NOT, not.op.getKind() );
        assertEquals( 1, not.operands.size() );
        RexCall subNot = assertRexCall( not, 0 );
        testJsonValueCond( subNot, "key.sub", 4, Kind.EQUALS );

        testJsonValueCond( lt, "key", 15, Kind.LESS_THAN );
    }


    @Test
    public void testFunctionalOperators() {
        AlgRoot root = translate( find( "\"key\": 2+3*10" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( 2, condition.operands.size() );
        assertEquals( Kind.EQUALS, condition.op.getKind() );

        RexCall calc = assertRexCall( condition, 1 );

        testJsonValue( assertRexCall( condition, 0 ), "key" );

        assertEquals( Kind.PLUS, calc.op.getKind() );
        assertEquals( 2, calc.operands.size() );

        testCastLiteral( calc.operands.get( 0 ), 2, Integer.class );
        RexCall mult = assertRexCall( calc, 1 );

        assertEquals( 2, mult.operands.size() );

        assertEquals( Kind.TIMES, mult.op.getKind() );
        testCastLiteral( mult.operands.get( 0 ), 3, Integer.class );
        testCastLiteral( mult.operands.get( 1 ), 10, Integer.class );
    }


    @Test
    public void testSingleTypeProjection() {
        // select only when key is string ( 2 )
        AlgRoot root = translate( find( "\"key\": {\"$type\":2}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( OperatorName.MQL_TYPE_MATCH, condition.op.getOperatorName() );
        assertEquals( 2, condition.operands.size() );

        RexCall query = assertRexCall( condition, 0 );
        testJsonValue( query, "key" );

        RexCall types = assertRexCall( condition, 1 );
        assertEquals( 1, types.operands.size() );
        assertEquals( Kind.ARRAY_VALUE_CONSTRUCTOR, types.getKind() );

        testCastLiteral( types.operands.get( 0 ), 2, Integer.class );
    }


    @Test
    public void testMultipleTypeProjection() {
        // select only when key is either int32 or int64
        AlgRoot root = translate( find( "\"key\": {\"$type\":[16, 18]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( OperatorName.MQL_TYPE_MATCH, condition.op.getOperatorName() );
        assertEquals( 2, condition.operands.size() );

        RexCall query = assertRexCall( condition, 0 );
        testJsonValue( query, "key" );

        RexCall types = assertRexCall( condition, 1 );
        assertEquals( 2, types.operands.size() );
        assertEquals( Kind.ARRAY_VALUE_CONSTRUCTOR, types.getKind() );

        testCastLiteral( types.operands.get( 0 ), 16, Integer.class );
        testCastLiteral( types.operands.get( 1 ), 18, Integer.class );
    }


    @Test
    public void testSingleExpressionProjection() {
        AlgRoot root = translate( find( "\"$expr\":{\"$lt\":[ \"$key1\", \"$key2\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( Kind.LESS_THAN, condition.op.getKind() );

        assertEquals( 2, condition.operands.size() );

        RexCall key1 = assertRexCall( condition, 0 );
        testJsonValue( key1, "key1" );

        RexCall key2 = assertRexCall( condition, 1 );
        testJsonValue( key2, "key2" );

    }


    @Test
    public void testLiteral() {
        AlgRoot root = translate( find( "\"key\": {$literal: 1}" ) );

        RexCall literal = getConditionTestFilter( root );
        testJsonValueCond( literal, "key", 1, Kind.EQUALS );
    }

    /////////// only projection /////////////


    @Test
    public void testEmptyProjection() {
        AlgRoot root = translate( find( "", "" ) );

        assertTrue( root.alg instanceof DocumentScan );

        DocumentScan scan = (DocumentScan) root.alg;
        //assertTrue( scan.getRowType().getFieldNames().contains( "_id" ) );
        assertTrue( scan.getRowType().getFieldNames().contains( "d" ) );
    }


    @Test
    public void testSingleInclusion() {
        AlgRoot root = translate( find( "", "\"key\": 1" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonValue( projection, "key" );

    }


    private RexNode getUncastUnderlyingProjection( AlgRoot root, int pos ) {
        Project project = getProject( root, pos );

        return project.getChildExps().get( pos );
    }


    private RexCall getUnderlyingProjection( AlgRoot root, int pos ) {
        Project project = getProject( root, pos );

        assertTrue( project.getChildExps().get( pos ) instanceof RexCall );
        return (RexCall) project.getChildExps().get( pos );
    }


    private Project getProject( AlgRoot root, int pos ) {
        assertTrue( root.alg instanceof Project );
        Project project = (Project) root.alg;

        assertTrue( project.getChildExps().size() >= pos );
        return project;
    }


    @Test
    public void testMultipleInclusion() {
        AlgRoot root = translate( find( "", "\"key\": 1, \"key1\": 1" ) );

        RexCall projection1 = getUnderlyingProjection( root, 1 );

        testJsonValue( projection1, "key" );

        RexCall projection2 = getUnderlyingProjection( root, 2 );

        testJsonValue( projection2, "key1" );

    }


    @Test
    public void testNestedInclusion() {
        AlgRoot root = translate( find( "", "\"key.subkey\": 1" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );
        testJsonValue( projection, "key.subkey" );

    }


    @Test
    public void testSingleExclusion() {
        AlgRoot root = translate( find( "", "\"key\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Collections.singletonList( "key" ) );
    }


    @Test
    public void testMultipleExclusion() {
        AlgRoot root = translate( find( "", "\"key\": 0, \"key1\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Arrays.asList( "key", "key1" ) );

    }


    @Test
    public void testMultipleNestedExclusion() {
        AlgRoot root = translate( find( "", "\"key.subkey\": 0, \"key1.subkey1\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Arrays.asList( "key.subkey", "key1.subkey1" ) );

    }


    @Test
    public void testMixedExAndInclusion() {
        boolean failed = false;
        try {
            AlgRoot root = translate( find( "", "\"key\": 1, \"key1\": 0" ) );
        } catch ( Exception e ) {
            failed = true;
        }
        if ( !failed ) {
            fail();
        }
    }


    @Test
    public void testNestedExclusion() {
        AlgRoot root = translate( find( "", "\"key.subkey\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Collections.singletonList( "key.subkey" ) );
    }


    @Test
    public void testSingleRename() {
        AlgRoot root = translate( find( "", "\"newName\": \"$key\"" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonValue( projection, "key" );

        assertTrue( root.validatedRowType.getFieldNames().contains( "newName" ) );
    }


    @Test
    public void testMultipleRename() {
        AlgRoot root = translate( find( "", "\"newName\": \"$key\", \"newName1\": \"$key1\"" ) );

        RexCall key = getUnderlyingProjection( root, 1 );

        testJsonValue( key, "key" );

        RexCall key1 = getUnderlyingProjection( root, 2 );

        testJsonValue( key1, "key1" );

        assertTrue( root.validatedRowType.getFieldNames().containsAll( Arrays.asList( "newName", "newName1" ) ) );
    }


    @Test
    public void testMixRenameAndInclusion() {
        AlgRoot root = translate( find( "", "\"newName\": \"$key\", \"key1\": 1" ) );

        RexCall key = getUnderlyingProjection( root, 1 );

        testJsonValue( key, "key" );

        RexCall key1 = getUnderlyingProjection( root, 2 );

        testJsonValue( key1, "key1" );

        assertTrue( root.validatedRowType.getFieldNames().containsAll( Arrays.asList( "newName", "key1" ) ) );
    }


    @Test
    public void testMathProjection() {
        AlgRoot root = translate( find( "", "\"key\": {\"$multiply\":[1,3]}" ) );

        RexCall condition = getUnderlyingProjection( root, 1 );

        assertEquals( Kind.TIMES, condition.op.getKind() );
        assertEquals( 2, condition.operands.size() );

        testCastLiteral( condition.operands.get( 0 ), 1, Integer.class );
        testCastLiteral( condition.operands.get( 1 ), 3, Integer.class );
    }


    private void testJsonQuery( String key, RexCall projection, List<String> excludes ) {
        assertEquals( OperatorName.MQL_EXCLUDE, projection.op.getOperatorName() );
        assertEquals( 2, projection.operands.size() );

        assertEquals( Kind.INPUT_REF, projection.operands.get( 0 ).getKind() );
        assertEquals( 1, ((RexInputRef) projection.operands.get( 0 )).getIndex() );

        RexCall arrayArray = assertRexCall( projection, 1 );

        assertEquals( Kind.ARRAY_VALUE_CONSTRUCTOR, arrayArray.op.getKind() );

        List<List<String>> excludedKeys = excludes
                .stream()
                .map( e -> Arrays.asList( e.split( "\\." ) ) )
                .collect( Collectors.toList() );

        int pos = 0;
        for ( RexNode array : arrayArray.operands ) {
            assertTrue( array instanceof RexCall );
            assertEquals( Kind.ARRAY_VALUE_CONSTRUCTOR, ((RexCall) array).op.getKind() );
            int innerPos = 0;
            for ( RexNode operand : ((RexCall) array).operands ) {
                testCastLiteral( operand, excludedKeys.get( pos ).get( innerPos ), String.class );
                innerPos++;
            }
            pos++;
        }
    }


    private RexCall assertRexCall( RexCall condition, int i ) {
        assertTrue( condition.operands.get( i ) instanceof RexCall );
        return (RexCall) condition.operands.get( i );
    }


    private void testNot( RexCall condition ) {
        assertEquals( Kind.NOT, condition.op.getKind() );
        assertEquals( 1, condition.operands.size() );
    }


    private RexCall getConditionTestFilter( AlgRoot root ) {
        assertTrue( root.alg instanceof DocumentFilter );
        DocumentFilter filter = ((DocumentFilter) root.alg);
        assertTrue( filter.getInput() instanceof DocumentScan );
        return (RexCall) filter.condition;
    }


    private void testJsonExists( RexCall condition, String key ) {
        assertEquals( Kind.MQL_EXISTS, condition.op.getKind() );
        assertEquals( 2, condition.operands.size() );

        assertEquals(
                Arrays.asList( key.split( "\\." ) ),
                assertRexCall( condition, 1 )
                        .operands
                        .stream()
                        .map( e -> ((RexLiteral) e).getValueAs( String.class ) )
                        .collect( Collectors.toList() ) );
    }


    private void testJsonValue( RexCall jsonValue, String key ) {
        assertEquals( OperatorName.MQL_QUERY_VALUE, jsonValue.op.getOperatorName() );

        assertEquals( 2, jsonValue.operands.size() );
        assertEquals( Kind.INPUT_REF, jsonValue.operands.get( 0 ).getKind() );
        assertEquals( 0, ((RexInputRef) jsonValue.operands.get( 0 )).getIndex() );

        RexCall array = assertRexCall( jsonValue, 0 );

        List<String> keys = Arrays.asList( key.split( "\\." ) );

        assertEquals( keys.size(), array.operands.size() );

        int pos = 0;
        for ( RexNode operand : array.operands ) {
            assertTrue( operand.isA( Kind.LITERAL ) );
            testCastLiteral( operand, keys.get( pos ), String.class );
            pos++;
        }
    }


    private void testJsonCommon( String key, RexCall jsonApi ) {
        testJsonCommon( key, jsonApi, new ArrayList<>() );
    }


    private void testJsonCommon( String key, RexCall jsonApi, List<String> excludes ) {
        assertEquals( Kind.JSON_API_COMMON_SYNTAX, jsonApi.op.getKind() );
        assertEquals( 2, jsonApi.operands.size() );

        RexCall jsonExpr = assertRexCall( jsonApi, 0 );
        if ( excludes.size() > 0 ) {
            testJsonExpressionExcludes( jsonExpr, excludes );
        } else {
            testJsonExpression( jsonExpr );
        }

        RexInputRef ref = (RexInputRef) jsonExpr.operands.get( 0 );
        assertEquals( 1, ref.getIndex() );

        // test json comp string

        assertTrue( jsonApi.operands.get( 1 ).isA( Kind.LITERAL ) );
        RexLiteral cond = (RexLiteral) jsonApi.operands.get( 1 );
        assertEquals( key == null ? "strict $" : "strict $." + key, cond.getValueAs( String.class ) );
    }


    private void testJsonExpressionExcludes( RexCall jsonExpr, List<String> excludes ) {
        assertEquals( Kind.JSON_VALUE_EXPRESSION, jsonExpr.op.getKind() );
        assertEquals( "JSON_VALUE_EXPRESSION_EXCLUDE", jsonExpr.op.getName() );
        assertEquals( 2, jsonExpr.operands.size() );
        assertTrue( jsonExpr.operands.get( 0 ).isA( Kind.INPUT_REF ) );

        // test exclusion array
        RexCall excluded = assertRexCall( jsonExpr, 1 );
        assertEquals( Kind.ARRAY_VALUE_CONSTRUCTOR, excluded.op.getKind() );
        assertEquals( excludes.size(), excluded.operands.size() );
        List<String> names = excluded.operands.stream()
                .map( e -> ((RexLiteral) e).getValueAs( String.class ) )
                .collect( Collectors.toList() );
        assertEquals( names, excludes );
    }


    private void testJsonExpression( RexCall jsonExpr ) {
        assertEquals( Kind.JSON_VALUE_EXPRESSION, jsonExpr.op.getKind() );
        assertEquals( 1, jsonExpr.operands.size() );
        assertTrue( jsonExpr.operands.get( 0 ).isA( Kind.INPUT_REF ) );
    }


    private void testCastLiteral( RexNode node, Object value, Class<?> clazz ) {
        assertTrue( node.isA( Kind.LITERAL ) );
        assertEquals( value, ((RexLiteral) node).getValueAs( clazz ) );
    }


    private void testJsonValueCond( RexCall condition, String key, Object value, Kind kind ) {
        assertEquals( kind, condition.op.getKind() );

       /*if ( Arrays.asList(
                Kind.GREATER_THAN,
                Kind.GREATER_THAN_OR_EQUAL,
                Kind.LESS_THAN,
                Kind.LESS_THAN_OR_EQUAL ).contains( kind ) ) {
            assertEquals( Kind.CAST, condition.operands.get( 0 ).getKind() );

            // test json value
            testJsonValue( assertRexCall(, 0 ), key );
        } else {*/
        // test json value
        testJsonValue( assertRexCall( condition, 0 ), key );
        //}

        // test initial comp value
        testCastLiteral( condition.operands.get( 1 ), value, value.getClass() );
    }


    private void testNoncastLiteral( RexCall jsonValue, int i, Object object ) {
        assertTrue( jsonValue.operands.get( i ).isA( Kind.LITERAL ) );
        RexLiteral flag = (RexLiteral) jsonValue.operands.get( i );
        assertEquals( object, flag.getValue() );
    }

}
