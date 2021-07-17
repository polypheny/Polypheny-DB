/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.mql2rel.dql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.junit.Test;
import org.polypheny.db.mql.MqlTest;
import org.polypheny.db.mql2rel.Mql2RelTest;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlJsonExistsFunction;

public class Mql2RelFindTest extends Mql2RelTest {

    public String find( String match ) {
        return "db.secrets.find({" + match + "})";
    }


    public String find( String match, String project ) {
        return "db.secrets.find({" + match + "},{" + project + "})";
    }


    @Test
    public void testEmptyMatch() {
        RelRoot root = translate( find( "" ) );
        assertTrue( root.rel instanceof TableScan );
    }


    @Test
    public void testSingleMatch() {
        RelRoot root = translate( find( "\"_id\":\"value\"" ) );
        RexCall condition = getConditionTestFilter( root );
        assertEquals( SqlKind.EQUALS, condition.op.kind );

        assertTrue( condition.operands.get( 0 ).isA( SqlKind.INPUT_REF ) );
        assertEquals( 0, ((RexInputRef) condition.operands.get( 0 )).getIndex() );

        testCastLiteral( condition.operands.get( 1 ), "value", String.class );
    }


    @Test
    public void testSingleMatchDocument() {
        RelRoot root = translate( find( "\"key\":\"value\"" ) );
        // test general structure
        RexCall condition = getConditionTestFilter( root );

        testJsonValueCond( condition, "key", "value", SqlKind.EQUALS );
    }


    @Test
    public void testMultipleMatchDocument() {
        RelRoot root = translate( find( "\"key\":\"value\",\"key1\":\"value1\"" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( SqlKind.AND, condition.op.kind );

        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1", "value1", SqlKind.EQUALS );
    }


    @Test
    public void testSingleNestedDocument() {
        RelRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( SqlKind.EQUALS, condition.op.kind );

        testJsonValueCond( condition, "key.key1", "value1", SqlKind.EQUALS );

        testCastLiteral( condition.operands.get( 1 ), "value1", String.class );
    }


    @Test
    public void testMultipleNestedDocument() {
        RelRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}, \"key1\":{\"key2\":\"value2\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( SqlKind.AND, condition.op.kind );

        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key.key1", "value1", SqlKind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1.key2", "value2", SqlKind.EQUALS );
    }


    @Test
    public void testSingleExists() {
        RelRoot root = translate( find( "\"key\": { \"$exists\": true}" ) );

        RexCall condition = getConditionTestFilter( root );

        testJsonExists( condition, "key" );
    }


    @Test
    public void testSingleNegativeExists() {
        RelRoot root = translate( find( "\"key\": { \"$exists\": false}" ) );

        RexCall condition = getConditionTestFilter( root );

        testNot( condition );

        RexCall json = assertRexCall( condition, 0 );
        testJsonExists( json, "key" );
    }


    @Test
    public void testMultipleExists() {
        RelRoot root = translate( find( "\"key\": { \"$exists\": true}, \"key1\":{ \"$exists\": false}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( SqlKind.AND, condition.op.kind );
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
        RelRoot root = translate( find( "\"key\": { \"$eq\": \"value\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        testJsonValueCond( condition, "key", "value", SqlKind.EQUALS );
    }


    @Test
    public void testBiComparisons() {
        // some make no sense, maybe fix in the future TODO DL
        for ( Entry<String, SqlKind> entry : MqlTest.getBiComparisons().entrySet() ) {
            RelRoot root = translate( find( "\"key\": { \"" + entry.getKey() + "\": \"value\"}" ) );

            RexCall condition = getConditionTestFilter( root );

            testJsonValueCond( condition, "key", "value", entry.getValue() );
        }
    }


    @Test
    public void testInOperator() {
        RelRoot root = translate( find( "\"key\": { \"$in\": [\"value\",\"value1\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( SqlKind.OR, condition.op.kind );
        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", SqlKind.EQUALS );
    }


    @Test
    public void testNinOperator() {
        RelRoot root = translate( find( "\"key\": { \"$nin\": [\"value\",\"value1\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( SqlKind.AND, condition.op.kind );
        assertEquals( 2, condition.operands.size() );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.NOT_EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", SqlKind.NOT_EQUALS );
    }


    @Test
    public void testLogicalAndOperator() {
        RelRoot root = translate( find( "\"key\": { \"$and\": [{\"$eq\": \"value\"},{\"$gt\": \"value1\"}]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( 2, condition.operands.size() );
        assertEquals( SqlKind.AND, condition.op.kind );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", SqlKind.GREATER_THAN );

        // less deep

        root = translate( find( " \"$and\": [{\"key\":\"value\"},{\"key1\":\"value1\"}]" ) );

        condition = getConditionTestFilter( root );

        assertEquals( 2, condition.operands.size() );
        assertEquals( SqlKind.AND, condition.op.kind );

        left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.EQUALS );

        right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1", "value1", SqlKind.EQUALS );
    }


    @Test
    public void testMixedNestedAndLogical() {
        RelRoot root = translate( find( "\"key\": {\"$gt\": 3, \"$and\": [{\"$not\":{ \"sub\": 4}},{\"$lt\": 15}]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( 3, condition.operands.size() );

        RexCall gt = assertRexCall( condition, 0 );
        RexCall not = assertRexCall( condition, 1 );
        RexCall lt = assertRexCall( condition, 2 );

        testJsonValueCond( gt, "key", 3, SqlKind.GREATER_THAN );
        assertEquals( SqlKind.NOT, not.op.kind );
        assertEquals( 1, not.operands.size() );
        RexCall subNot = assertRexCall( not, 0 );
        testJsonValueCond( subNot, "key.sub", 4, SqlKind.EQUALS );

        testJsonValueCond( lt, "key", 15, SqlKind.LESS_THAN );
    }


    @Test
    public void testFunctionalOperators() {
        RelRoot root = translate( find( "\"key\": 2-3*10" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( 2, condition.operands.size() );
        assertEquals( SqlKind.EQUALS, condition.op.kind );

        RexCall calc = assertRexCall( condition, 1 );

        testJsonValue( assertRexCall( condition, 0 ), "key" );

        assertEquals( SqlKind.MINUS, calc.op.kind );
        assertEquals( 2, calc.operands.size() );

        testCastLiteral( calc.operands.get( 0 ), 2, Integer.class );
        RexCall mult = assertRexCall( calc, 1 );

        assertEquals( 2, mult.operands.size() );

        assertEquals( SqlKind.TIMES, mult.op.kind );
        testCastLiteral( mult.operands.get( 0 ), 3, Integer.class );
        testCastLiteral( mult.operands.get( 1 ), 10, Integer.class );
    }


    @Test
    public void testSingleTypeProjection() {
        // select only when key is string ( 2 )
        RelRoot root = translate( find( "\"key\": {\"$type\":2}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( "DOC_TYPE_MATCH", condition.op.getName() );
        assertEquals( 2, condition.operands.size() );

        RexCall query = assertRexCall( condition, 0 );
        testJsonValue( query, "key" );

        RexCall types = assertRexCall( condition, 1 );
        assertEquals( 1, types.operands.size() );
        assertEquals( SqlKind.ARRAY_VALUE_CONSTRUCTOR, types.getKind() );

        testCastLiteral( types.operands.get( 0 ), 2, Integer.class );
    }


    @Test
    public void testMultipleTypeProjection() {
        // select only when key is either int32 or int64
        RelRoot root = translate( find( "\"key\": {\"$type\":[16, 18]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( "DOC_TYPE_MATCH", condition.op.getName() );
        assertEquals( 2, condition.operands.size() );

        RexCall query = assertRexCall( condition, 0 );
        testJsonValue( query, "key" );

        RexCall types = assertRexCall( condition, 1 );
        assertEquals( 2, types.operands.size() );
        assertEquals( SqlKind.ARRAY_VALUE_CONSTRUCTOR, types.getKind() );

        testCastLiteral( types.operands.get( 0 ), 16, Integer.class );
        testCastLiteral( types.operands.get( 1 ), 18, Integer.class );
    }


    @Test
    public void testSingleExpressionProjection() {
        RelRoot root = translate( find( "\"$expr\":{\"$lt\":[ \"$key1\", \"$key2\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( SqlKind.LESS_THAN, condition.op.kind );

        assertEquals( 2, condition.operands.size() );

        RexCall key1 = assertRexCall( condition, 0 );
        testJsonValue( key1, "key1" );

        RexCall key2 = assertRexCall( condition, 1 );
        testJsonValue( key2, "key2" );

    }

    /////////// only projection /////////////


    @Test
    public void testEmptyProjection() {
        RelRoot root = translate( find( "", "" ) );

        assertTrue( root.rel instanceof TableScan );

        TableScan scan = (TableScan) root.rel;
        assertTrue( scan.getRowType().getFieldNames().contains( "_id" ) );
        assertTrue( scan.getRowType().getFieldNames().contains( "_data" ) );
    }


    @Test
    public void testSingleInclusion() {
        RelRoot root = translate( find( "", "\"key\": 1" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonValue( projection, "key" );

    }


    private RexNode getUncastUnderlyingProjection( RelRoot root, int pos ) {
        Project project = getProject( root, pos );

        return project.getChildExps().get( pos );
    }


    private RexCall getUnderlyingProjection( RelRoot root, int pos ) {
        Project project = getProject( root, pos );

        assertTrue( project.getChildExps().get( pos ) instanceof RexCall );
        return (RexCall) project.getChildExps().get( pos );
    }


    private Project getProject( RelRoot root, int pos ) {
        assertTrue( root.rel instanceof Project );
        Project project = (Project) root.rel;

        assertTrue( project.getChildExps().size() >= pos );
        return project;
    }


    @Test
    public void testMultipleInclusion() {
        RelRoot root = translate( find( "", "\"key\": 1, \"key1\": 1" ) );

        RexCall projection1 = getUnderlyingProjection( root, 2 );

        testJsonValue( projection1, "key" );

        RexCall projection2 = getUnderlyingProjection( root, 1 );

        testJsonValue( projection2, "key1" );

    }


    @Test
    public void testNestedInclusion() {
        RelRoot root = translate( find( "", "\"key.subkey\": 1" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );
        testJsonValue( projection, "key.subkey" );

    }


    @Test
    public void testSingleExclusion() {
        RelRoot root = translate( find( "", "\"key\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Collections.singletonList( "key" ) );
    }


    @Test
    public void testMultipleExclusion() {
        RelRoot root = translate( find( "", "\"key\": 0, \"key1\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Arrays.asList( "key", "key1" ) );

    }


    @Test
    public void testMultipleNestedExclusion() {
        RelRoot root = translate( find( "", "\"key.subkey\": 0, \"key1.subkey1\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Arrays.asList( "key.subkey", "key1.subkey1" ) );

    }


    @Test
    public void testMixedExAndInclusion() {
        boolean failed = false;
        try {
            RelRoot root = translate( find( "", "\"key\": 1, \"key1\": 0" ) );
        } catch ( Exception e ) {
            failed = true;
        }
        if ( !failed ) {
            fail();
        }
    }


    @Test
    public void testNestedExclusion() {
        RelRoot root = translate( find( "", "\"key.subkey\": 0" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonQuery( null, projection, Collections.singletonList( "key.subkey" ) );
    }


    @Test
    public void testSingleRename() {
        RelRoot root = translate( find( "", "\"newName\": \"$key\"" ) );

        RexCall projection = getUnderlyingProjection( root, 1 );

        testJsonValue( projection, "key" );

        assertTrue( root.validatedRowType.getFieldNames().contains( "newName" ) );
    }


    @Test
    public void testMultipleRename() {
        RelRoot root = translate( find( "", "\"newName\": \"$key\", \"newName1\": \"$key1\"" ) );

        RexCall key = getUnderlyingProjection( root, 1 );

        testJsonValue( key, "key" );

        RexCall key1 = getUnderlyingProjection( root, 2 );

        testJsonValue( key1, "key1" );

        assertTrue( root.validatedRowType.getFieldNames().containsAll( Arrays.asList( "newName", "newName1" ) ) );
    }


    @Test
    public void testMixRenameAndInclusion() {
        RelRoot root = translate( find( "", "\"newName\": \"$key\", \"key1\": 1" ) );

        RexCall key = getUnderlyingProjection( root, 2 );

        testJsonValue( key, "key" );

        RexCall key1 = getUnderlyingProjection( root, 1 );

        testJsonValue( key1, "key1" );

        assertTrue( root.validatedRowType.getFieldNames().containsAll( Arrays.asList( "newName", "key1" ) ) );
    }


    @Test
    public void testLiteral() {
        RelRoot root = translate( find( "", "\"key\": {\"$literal\": 1}" ) );

        RexNode literal = getUncastUnderlyingProjection( root, 1 );
        assertTrue( literal instanceof RexLiteral );

        testCastLiteral( literal, 1, Integer.class );
    }


    @Test
    public void testMathProjection() {
        RelRoot root = translate( find( "", "\"key\": {\"$multiply\":[1,3]}" ) );

        RexCall condition = getUnderlyingProjection( root, 1 );

        assertEquals( SqlKind.TIMES, condition.op.kind );
        assertEquals( 2, condition.operands.size() );

        testCastLiteral( condition.operands.get( 0 ), 1, Integer.class );
        testCastLiteral( condition.operands.get( 1 ), 3, Integer.class );
    }


    private void testJsonQuery( String key, RexCall projection, List<String> excludes ) {
        assertEquals( "DOC_QUERY_EXCLUDE", projection.op.getName() );
        assertEquals( 2, projection.operands.size() );

        assertEquals( SqlKind.INPUT_REF, projection.operands.get( 0 ).getKind() );
        assertEquals( 1, ((RexInputRef) projection.operands.get( 0 )).getIndex() );

        RexCall arrayArray = assertRexCall( projection, 1 );

        assertEquals( SqlKind.ARRAY_VALUE_CONSTRUCTOR, arrayArray.op.kind );

        List<List<String>> excludedKeys = excludes
                .stream()
                .map( e -> Arrays.asList( e.split( "\\." ) ) )
                .collect( Collectors.toList() );

        int pos = 0;
        for ( RexNode array : arrayArray.operands ) {
            assertTrue( array instanceof RexCall );
            assertEquals( SqlKind.ARRAY_VALUE_CONSTRUCTOR, ((RexCall) array).op.kind );
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
        assertEquals( SqlKind.NOT, condition.op.kind );
        assertEquals( 1, condition.operands.size() );
    }


    private RexCall getConditionTestFilter( RelRoot root ) {
        assertTrue( root.rel instanceof Filter );
        Filter filter = ((Filter) root.rel);
        assertTrue( filter.getInput() instanceof TableScan );
        return (RexCall) filter.getCondition();
    }


    private void testJsonExists( RexCall condition, String key ) {
        assertTrue( condition.op instanceof SqlJsonExistsFunction );
        assertEquals( 1, condition.operands.size() );

        RexCall common = assertRexCall( condition, 0 );
        testJsonCommon( key, common );
    }


    private void testJsonValue( RexCall jsonValue, String key ) {
        assertEquals( "DOC_QUERY_VALUE", jsonValue.op.getName() );

        assertEquals( 2, jsonValue.operands.size() );
        assertEquals( SqlKind.INPUT_REF, jsonValue.operands.get( 0 ).getKind() );
        assertEquals( 1, ((RexInputRef) jsonValue.operands.get( 0 )).getIndex() );

        RexCall array = assertRexCall( jsonValue, 1 );

        List<String> keys = Arrays.asList( key.split( "\\." ) );

        assertEquals( keys.size(), array.operands.size() );

        int pos = 0;
        for ( RexNode operand : array.operands ) {
            assertTrue( operand.isA( SqlKind.LITERAL ) );
            testCastLiteral( operand, keys.get( pos ), String.class );
            pos++;
        }
    }


    private void testJsonCommon( String key, RexCall jsonApi ) {
        testJsonCommon( key, jsonApi, new ArrayList<>() );
    }


    private void testJsonCommon( String key, RexCall jsonApi, List<String> excludes ) {
        assertEquals( SqlKind.JSON_API_COMMON_SYNTAX, jsonApi.op.kind );
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

        assertTrue( jsonApi.operands.get( 1 ).isA( SqlKind.LITERAL ) );
        RexLiteral cond = (RexLiteral) jsonApi.operands.get( 1 );
        assertEquals( key == null ? "strict $" : "strict $." + key, cond.getValueAs( String.class ) );
    }


    private void testJsonExpressionExcludes( RexCall jsonExpr, List<String> excludes ) {
        assertEquals( SqlKind.JSON_VALUE_EXPRESSION, jsonExpr.op.kind );
        assertEquals( "JSON_VALUE_EXPRESSION_EXCLUDE", jsonExpr.op.getName() );
        assertEquals( 2, jsonExpr.operands.size() );
        assertTrue( jsonExpr.operands.get( 0 ).isA( SqlKind.INPUT_REF ) );

        // test exclusion array
        RexCall excluded = assertRexCall( jsonExpr, 1 );
        assertEquals( SqlKind.ARRAY_VALUE_CONSTRUCTOR, excluded.op.kind );
        assertEquals( excludes.size(), excluded.operands.size() );
        List<String> names = excluded.operands.stream()
                .map( e -> ((RexLiteral) e).getValueAs( String.class ) )
                .collect( Collectors.toList() );
        assertEquals( names, excludes );
    }


    private void testJsonExpression( RexCall jsonExpr ) {
        assertEquals( SqlKind.JSON_VALUE_EXPRESSION, jsonExpr.op.kind );
        assertEquals( 1, jsonExpr.operands.size() );
        assertTrue( jsonExpr.operands.get( 0 ).isA( SqlKind.INPUT_REF ) );
    }


    private void testCastLiteral( RexNode node, Object value, Class<?> clazz ) {
        assertTrue( node.isA( SqlKind.LITERAL ) );
        assertEquals( value, ((RexLiteral) node).getValueAs( clazz ) );
    }


    private void testJsonValueCond( RexCall condition, String key, Object value, SqlKind kind ) {
        assertEquals( kind, condition.op.kind );

        // test json value
        testJsonValue( assertRexCall( condition, 0 ), key );

        // test initial comp value
        testCastLiteral( condition.operands.get( 1 ), value, value.getClass() );
    }


    private void testNoncastLiteral( RexCall jsonValue, int i, Object object ) {
        assertTrue( jsonValue.operands.get( i ).isA( SqlKind.LITERAL ) );
        RexLiteral flag = (RexLiteral) jsonValue.operands.get( i );
        assertEquals( object, flag.getValue() );
    }

}
