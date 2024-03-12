/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.cql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cql.ColumnFilter;
import org.polypheny.db.cql.Comparator;
import org.polypheny.db.cql.FieldIndex;
import org.polypheny.db.cql.LiteralFilter;
import org.polypheny.db.cql.Relation;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.utils.helper.AlgBuildTestHelper;
import org.polypheny.db.rex.RexNode;


public class FilterTest extends AlgBuildTestHelper {

    private final AlgNode baseNode;
    private final Map<String, AlgDataTypeField> filterMap = new HashMap<>();


    public FilterTest() throws UnknownIndexException {
        super( AlgBuildLevel.INITIAL_PROJECTION );
        baseNode = algBuilder.peek();
        AlgDataType filtersRowType = baseNode.getTupleType();
        List<AlgDataTypeField> filtersRows = filtersRowType.getFields();
        filtersRows.forEach( ( r ) -> filterMap.put( r.getName(), r ) );
    }


    @Test
    public void testColumnFilterThrowsNotImplementedRuntimeException() throws UnknownIndexException {

        RuntimeException thrown = assertThrows( RuntimeException.class, () -> {
            ColumnFilter columnFilter = new ColumnFilter(
                    FieldIndex.createIndex( "test", "dept", "deptno" ),
                    new Relation( Comparator.EQUALS ),
                    FieldIndex.createIndex( "test", "employee", "deptno" )
            );
            columnFilter.convert2RexNode( baseNode, rexBuilder, filterMap );
        } );
    }


    @Test
    public void testLiteralFilter() throws UnknownIndexException {
        LiteralFilter literalFilter = new LiteralFilter(
                FieldIndex.createIndex( "test", "employee", "deptno" ),
                new Relation( Comparator.EQUALS ),
                "1"
        );
        RexNode rexNode = literalFilter.convert2RexNode( baseNode, rexBuilder, filterMap );
        assertEquals( Kind.EQUALS, rexNode.getKind() );
    }

}
