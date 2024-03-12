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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.cql.FieldIndex;
import org.polypheny.db.cql.Modifier;
import org.polypheny.db.cql.Projections;
import org.polypheny.db.cql.Projections.AggregationFunctions;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.utils.helper.AlgBuildTestHelper;


public class ProjectionsTest extends AlgBuildTestHelper {

    private final Projections projections;
    private final FieldIndex empname;
    private final FieldIndex deptname;
    private final Map<String, Modifier> empnameModifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
    private final Map<String, Modifier> deptnameModifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );


    public ProjectionsTest() throws UnknownIndexException {
        super( AlgBuildLevel.INITIAL_PROJECTION );
        projections = new Projections();
        empname = FieldIndex.createIndex( "test", "employee", "empname" );
        deptname = FieldIndex.createIndex( "test", "dept", "deptname" );
    }


    @Test
    public void testGetAggregateFunction() throws UnknownIndexException {
        Map<String, String> testCases = new HashMap<>();
        testCases.put( "cOuNt", "count" );
        testCases.put( "Max", "max" );
        testCases.put( "miN", "min" );
        testCases.put( "sUm", "sum" );
        testCases.put( "AVG", "avg" );
        testCases.put( "", null );
        testCases.put( "abc", null );

        FieldIndex fieldIndex = FieldIndex.createIndex( "test", "employee", "empname" );

        testCases.forEach( ( modifierName, expected ) ->
                testGetAggregateFunctionHelper( fieldIndex, modifierName, expected ) );
    }


    @Test
    public void testConvert2RelWithoutAggregations() {
        Projections projections = new Projections();
        projections.add( empname, empnameModifiers );
        projections.add( deptname, deptnameModifiers );
        algBuilder = projections.convert2Rel( tableScanOrdinalities, algBuilder, rexBuilder );
        AlgNode algNode = algBuilder.peek();
        List<String> actualFieldNames = algNode.getTupleType().getFieldNames();
        List<String> expectedFieldNames = new ArrayList<>();
        expectedFieldNames.add( "test.employee.empname" );
        expectedFieldNames.add( "test.dept.deptname" );

        assertEquals( expectedFieldNames, actualFieldNames );
    }


    @Test
    public void testConvert2RelWithAggregationWithoutGrouping() {
        empnameModifiers.put( "count", new Modifier( "count" ) );
        projections.add( empname, empnameModifiers );
        algBuilder = projections.convert2Rel( tableScanOrdinalities, algBuilder, rexBuilder );
        AlgNode algNode = algBuilder.peek();
        List<String> actualFieldNames = algNode.getTupleType().getFieldNames();
        String actualFieldName = actualFieldNames.get( 0 );
        String expectedFieldName = AggregationFunctions.COUNT.getAliasWithColumnName( empname.fullyQualifiedName );

        assertEquals( 1, actualFieldNames.size() );
        assertEquals( expectedFieldName, actualFieldName );
    }


    @Test
    public void testConvert2RelWithAggregationAndGrouping() {
        empnameModifiers.put( "count", new Modifier( "count" ) );
        projections.add( empname, empnameModifiers );
        projections.add( deptname, deptnameModifiers );
        algBuilder = projections.convert2Rel( tableScanOrdinalities, algBuilder, rexBuilder );
        AlgNode algNode = algBuilder.peek();
        List<String> actualFieldNames = algNode.getTupleType().getFieldNames();
        List<String> expectedFiledNames = new ArrayList<>();
        expectedFiledNames.add( AggregationFunctions.COUNT.getAliasWithColumnName( empname.fullyQualifiedName ) );
        expectedFiledNames.add( deptname.fullyQualifiedName );

        assertEquals( expectedFiledNames, actualFieldNames );
    }


    private void testGetAggregateFunctionHelper( FieldIndex fieldIndex, String modifierName, String expected ) {
        Map<String, Modifier> modifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        modifiers.put( modifierName, new Modifier( modifierName ) );
        String actual = Projections.getAggregationFunction( fieldIndex, modifiers );
        assertEquals( expected, actual );
    }

}
