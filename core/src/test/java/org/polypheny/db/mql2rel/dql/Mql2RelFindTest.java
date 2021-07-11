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

import org.junit.Test;
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
        assert (root.rel instanceof TableScan);
    }


    @Test
    public void testSingleMatch() {
        RelRoot root = translate( find( "\"_id\":\"value\"" ) );
        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();
        assert (condition.op.kind == SqlKind.EQUALS);

        assert (condition.operands.get( 0 ).isA( SqlKind.INPUT_REF ));
        assert (((RexInputRef) condition.operands.get( 0 )).getIndex() == 0);

        testCastLiteral( condition.operands.get( 1 ), "value", String.class );
    }


    @Test
    public void testSingleMatchDocument() {
        RelRoot root = translate( find( "\"key\":\"value\"" ) );
        // test general structure
        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();

        testJsonValueEquals( condition, "key", "value" );
    }


    @Test
    public void testMultipleMatchDocument() {
        RelRoot root = translate( find( "\"key\":\"value\",\"key1\":\"value1\"" ) );

        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();
        assert (condition.op.kind == SqlKind.AND);

        assert (condition.operands.size() == 2);

        assert (condition.operands.get( 0 ) instanceof RexCall);
        RexCall left = (RexCall) condition.operands.get( 0 );
        testJsonValueEquals( left, "key", "value" );

        assert (condition.operands.get( 1 ) instanceof RexCall);
        RexCall right = (RexCall) condition.operands.get( 1 );
        testJsonValueEquals( right, "key1", "value1" );
    }


    @Test
    public void testSingleNestedDocument() {
        RelRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}" ) );

        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();
        assert (condition.op.kind == SqlKind.EQUALS);

        testJsonValueEquals( condition, "key.key1", "value1" );

        testCastLiteral( condition.operands.get( 1 ), "value1", String.class );
    }


    @Test
    public void testMultipleNestedDocument() {
        RelRoot root = translate( find( "\"key\":{\"key1\":\"value1\"}, \"key1\":{\"key2\":\"value2\"}" ) );

        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();
        assert (condition.op.kind == SqlKind.AND);

        assert (condition.operands.size() == 2);

        assert (condition.operands.get( 0 ) instanceof RexCall);
        RexCall left = (RexCall) condition.operands.get( 0 );
        testJsonValueEquals( left, "key.key1", "value1" );

        assert (condition.operands.get( 1 ) instanceof RexCall);
        RexCall right = (RexCall) condition.operands.get( 1 );
        testJsonValueEquals( right, "key1.key2", "value2" );
    }


    @Test
    public void testSingleExists() {
        RelRoot root = translate( find( "\"key\": { \"$exists\": true}" ) );

        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();

        testJsonExists( condition );
    }


    @Test
    public void testSingleNegativeExists() {
        RelRoot root = translate( find( "\"key\": { \"$exists\": false}" ) );

        assert (root.rel instanceof Filter);
        Filter filter = ((Filter) root.rel);
        assert (filter.getInput() instanceof TableScan);
        RexCall condition = (RexCall) filter.getCondition();

        assert (condition.op.kind == SqlKind.NOT);
        assert (condition.operands.size() == 1);
        assert (condition.operands.get( 0 ) instanceof RexCall);
        RexCall json = (RexCall) condition.operands.get( 0 );

        testJsonExists( json );
    }


    private void testJsonExists( RexCall condition ) {
        assert (condition.op instanceof SqlJsonExistsFunction);
        assert (condition.operands.size() == 1);
        assert (condition.operands.get( 0 ) instanceof RexCall);
        RexCall common = (RexCall) condition.operands.get( 0 );
        testJsonCommon( "key", common );
    }


    private void testJsonValue( RexCall cast, String key ) {
        assert (cast.operands.size() == 1);
        assert (cast.operands.get( 0 ) instanceof RexCall);
        RexCall jsonValue = (RexCall) cast.operands.get( 0 );
        assert (jsonValue.op instanceof SqlJsonValueFunction);

        // test json ref logic
        assert (jsonValue.operands.size() == 5);
        assert (jsonValue.operands.get( 0 ) instanceof RexCall);

        RexCall jsonApi = (RexCall) jsonValue.operands.get( 0 );
        testJsonCommon( key, jsonApi );

        testNoncastLiteral( jsonValue, 1, SqlJsonValueEmptyOrErrorBehavior.NULL );

        testNoncastLiteral( jsonValue, 2, null );

        testNoncastLiteral( jsonValue, 3, SqlJsonValueEmptyOrErrorBehavior.NULL );

        testNoncastLiteral( jsonValue, 4, null );
    }


    private void testJsonCommon( String key, RexCall jsonApi ) {
        assert (jsonApi.op.kind == SqlKind.JSON_API_COMMON_SYNTAX);
        assert (jsonApi.operands.size() == 2);
        assert (jsonApi.operands.get( 0 ) instanceof RexCall);
        RexCall jsonExpr = (RexCall) jsonApi.operands.get( 0 );
        assert (jsonExpr.op.kind == SqlKind.JSON_VALUE_EXPRESSION);
        assert (jsonExpr.operands.size() == 1);
        assert (jsonExpr.operands.get( 0 ).isA( SqlKind.INPUT_REF ));
        RexInputRef ref = (RexInputRef) jsonExpr.operands.get( 0 );
        assert (ref.getIndex() == 1);

        // test json comp string

        assert (jsonApi.operands.get( 1 ).isA( SqlKind.LITERAL ));
        RexLiteral cond = (RexLiteral) jsonApi.operands.get( 1 );
        assert (cond.getValueAs( String.class ).equals( "strict $." + key ));
    }


    private void testCastLiteral( RexNode node, Object value, Class<?> clazz ) {
        assert (node.isA( SqlKind.LITERAL ));
        assert (((RexLiteral) node).getValueAs( clazz ).equals( value ));
    }


    private void testJsonValueEquals( RexCall condition, String key, String value ) {
        assert (condition.op.kind == SqlKind.EQUALS);

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
