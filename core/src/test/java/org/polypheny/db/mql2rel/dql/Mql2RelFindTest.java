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

import java.util.Map.Entry;
import org.junit.Test;
import org.polypheny.db.mql.MqlTest;
import org.polypheny.db.mql2rel.Mql2RelTest;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlJsonExistsFunction;
import org.polypheny.db.sql.fun.SqlJsonValueFunction;

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
        assertEquals( condition.op.kind, SqlKind.EQUALS );

        assertTrue( condition.operands.get( 0 ).isA( SqlKind.INPUT_REF ) );
        assertEquals( ((RexInputRef) condition.operands.get( 0 )).getIndex(), 0 );

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
        assertEquals( condition.op.kind, SqlKind.AND );

        assertEquals( condition.operands.size(), 2 );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key1", "value1", SqlKind.EQUALS );
    }


    @Test
    public void testSingleNestedDocument() {
        RelRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( condition.op.kind, SqlKind.EQUALS );

        testJsonValueCond( condition, "key.key1", "value1", SqlKind.EQUALS );

        testCastLiteral( condition.operands.get( 1 ), "value1", String.class );
    }


    @Test
    public void testMultipleNestedDocument() {
        RelRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}, \"key1\":{\"key2\":\"value2\"}" ) );

        RexCall condition = getConditionTestFilter( root );
        assertEquals( condition.op.kind, SqlKind.AND );

        assertEquals( condition.operands.size(), 2 );

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

        assertEquals( condition.op.kind, SqlKind.AND );
        assertEquals( condition.operands.size(), 2 );
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

        assertEquals( condition.op.kind, SqlKind.OR );
        assertEquals( condition.operands.size(), 2 );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", SqlKind.EQUALS );
    }


    @Test
    public void TestNinOperator() {
        RelRoot root = translate( find( "\"key\": { \"$nin\": [\"value\",\"value1\"]}" ) );

        RexCall condition = getConditionTestFilter( root );

        assertEquals( condition.op.kind, SqlKind.AND );
        assertEquals( condition.operands.size(), 2 );

        RexCall left = assertRexCall( condition, 0 );
        testJsonValueCond( left, "key", "value", SqlKind.NOT_EQUALS );

        RexCall right = assertRexCall( condition, 1 );
        testJsonValueCond( right, "key", "value1", SqlKind.NOT_EQUALS );
    }


    private RexCall assertRexCall( RexCall condition, int i ) {
        assertTrue( condition.operands.get( i ) instanceof RexCall );
        return (RexCall) condition.operands.get( i );
    }


    private void testNot( RexCall condition ) {
        assertEquals( condition.op.kind, SqlKind.NOT );
        assertEquals( condition.operands.size(), 1 );
    }


    private RexCall getConditionTestFilter( RelRoot root ) {
        assertTrue( root.rel instanceof Filter );
        Filter filter = ((Filter) root.rel);
        assertTrue( filter.getInput() instanceof TableScan );
        return (RexCall) filter.getCondition();
    }


    private void testJsonExists( RexCall condition, String key ) {
        assertTrue( condition.op instanceof SqlJsonExistsFunction );
        assertEquals( condition.operands.size(), 1 );

        RexCall common = assertRexCall( condition, 0 );
        testJsonCommon( key, common );
    }


    private void testJsonValue( RexCall cast, String key ) {
        assertEquals( cast.operands.size(), 1 );

        RexCall jsonValue = assertRexCall( cast, 0 );
        assertTrue( jsonValue.op instanceof SqlJsonValueFunction );

        // test json ref logic
        assertEquals( jsonValue.operands.size(), 5 );

        RexCall jsonApi = assertRexCall( jsonValue, 0 );
        testJsonCommon( key, jsonApi );

        testNoncastLiteral( jsonValue, 1, SqlJsonValueEmptyOrErrorBehavior.NULL );
        testNoncastLiteral( jsonValue, 2, null );
        testNoncastLiteral( jsonValue, 3, SqlJsonValueEmptyOrErrorBehavior.NULL );
        testNoncastLiteral( jsonValue, 4, null );
    }


    private void testJsonCommon( String key, RexCall jsonApi ) {
        assertEquals( jsonApi.op.kind, SqlKind.JSON_API_COMMON_SYNTAX );
        assertEquals( jsonApi.operands.size(), 2 );

        RexCall jsonExpr = assertRexCall( jsonApi, 0 );
        assertEquals( jsonExpr.op.kind, SqlKind.JSON_VALUE_EXPRESSION );
        assertEquals( jsonExpr.operands.size(), 1 );
        assertTrue( jsonExpr.operands.get( 0 ).isA( SqlKind.INPUT_REF ) );
        RexInputRef ref = (RexInputRef) jsonExpr.operands.get( 0 );
        assertEquals( ref.getIndex(), 1 );

        // test json comp string

        assertTrue( jsonApi.operands.get( 1 ).isA( SqlKind.LITERAL ) );
        RexLiteral cond = (RexLiteral) jsonApi.operands.get( 1 );
        assertEquals( cond.getValueAs( String.class ), "strict $." + key );
    }


    private void testCastLiteral( RexNode node, Object value, Class<?> clazz ) {
        assert (node.isA( SqlKind.LITERAL ));
        assert (((RexLiteral) node).getValueAs( clazz ).equals( value ));
    }


    private void testJsonValueCond( RexCall condition, String key, String value, SqlKind kind ) {
        assert (condition.op.kind == kind);

        // test json value
        assert (condition.operands.get( 0 ).isA( SqlKind.CAST ));
        RexCall cast = (RexCall) condition.operands.get( 0 );
        testJsonValue( cast, key );

        // test initial comp value
        testCastLiteral( condition.operands.get( 1 ), value, String.class );
    }


    private void testNoncastLiteral( RexCall jsonValue, int i, Object object ) {
        assert (jsonValue.operands.get( i ).isA( SqlKind.LITERAL ));
        RexLiteral flag1 = (RexLiteral) jsonValue.operands.get( i );
        assert (flag1.getValue() == object);
    }

}
