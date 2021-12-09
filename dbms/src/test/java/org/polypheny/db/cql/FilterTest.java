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

package org.polypheny.db.cql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.helper.AlgBuildTestHelper;
import org.polypheny.db.rex.RexNode;


public class FilterTest extends AlgBuildTestHelper {

    private final AlgNode baseNode;
    private final Map<String, AlgDataTypeField> filterMap = new HashMap<>();


    public FilterTest() throws UnknownIndexException {
        super( AlgBuildLevel.INITIAL_PROJECTION );
        baseNode = algBuilder.peek();
        AlgDataType filtersRowType = baseNode.getRowType();
        List<AlgDataTypeField> filtersRows = filtersRowType.getFieldList();
        filtersRows.forEach( ( r ) -> filterMap.put( r.getKey(), r ) );
    }


    @Test(expected = RuntimeException.class)
    public void testColumnFilterThrowsNotImplementedRuntimeException() throws UnknownIndexException {
        ColumnFilter columnFilter = new ColumnFilter(
                ColumnIndex.createIndex( "APP", "test", "dept", "deptno" ),
                new Relation( Comparator.EQUALS ),
                ColumnIndex.createIndex( "APP", "test", "employee", "deptno" )
        );
        columnFilter.convert2RexNode( baseNode, rexBuilder, filterMap );
    }


    @Test
    public void testLiteralFilter() throws UnknownIndexException {
        LiteralFilter literalFilter = new LiteralFilter(
                ColumnIndex.createIndex( "APP", "test", "employee", "deptno" ),
                new Relation( Comparator.EQUALS ),
                "1"
        );
        RexNode rexNode = literalFilter.convert2RexNode( baseNode, rexBuilder, filterMap );
        Assert.assertEquals( Kind.EQUALS, rexNode.getKind() );
    }

}
