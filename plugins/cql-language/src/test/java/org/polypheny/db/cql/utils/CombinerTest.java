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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.cql.BooleanGroup;
import org.polypheny.db.cql.BooleanGroup.EntityOpsBooleanOperator;
import org.polypheny.db.cql.Combiner;
import org.polypheny.db.cql.Combiner.CombinerType;
import org.polypheny.db.cql.Comparator;
import org.polypheny.db.cql.EntityIndex;
import org.polypheny.db.cql.Modifier;
import org.polypheny.db.cql.exception.InvalidModifierException;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.utils.helper.AlgBuildTestHelper;


public class CombinerTest extends AlgBuildTestHelper {

    private final EntityIndex employee;
    private final EntityIndex dept;


    public CombinerTest() throws UnknownIndexException {
        super( AlgBuildLevel.TABLE_SCAN );
        employee = EntityIndex.createIndex( "test", "employee" );
        dept = EntityIndex.createIndex( "test", "dept" );
    }


    @Test
    public void testANDCombinerWithNoModifiers() throws InvalidModifierException {
        testANDCombinerHelper( false, "", new String[]{ "deptno" } );
    }


    @Test
    public void testANDCombinerWithALLOnModifier() throws InvalidModifierException {
        testANDCombinerHelper( true, "alL", new String[]{ "deptno" } );
    }


    @Test
    public void testANDCombinerWithNONEOnModifier() throws InvalidModifierException {
        testANDCombinerHelper( true, "nOnE", new String[0] );
    }


    @Test
    public void testANDCombinerWithColumnListOnModifier() throws InvalidModifierException {
        testANDCombinerHelper( true, "deptno", new String[]{ "deptno" } );
    }


    @Test
    public void testORCombinerWithNoModifiers() throws InvalidModifierException {
        testORCombinerHelper( null, null, new String[0], CombinerType.JOIN_FULL );
    }


    @Test
    public void testORCombinerWithALLOnModifier() throws InvalidModifierException {
        testORCombinerHelper( "ALl", null, new String[]{ "deptno" }, CombinerType.JOIN_FULL );
    }


    @Test
    public void testORCombinerWithNONEOnModifier() throws InvalidModifierException {
        testORCombinerHelper( "NoNe", null, new String[0], CombinerType.JOIN_FULL );
    }


    @Test
    public void testORCombinerWithLEFTNullModifier() throws InvalidModifierException {
        testORCombinerHelper( null, "LefT", new String[0], CombinerType.JOIN_RIGHT );
    }


    @Test
    public void testORCombinerWithALLOnAndLEFTNullModifiers() throws InvalidModifierException {
        testORCombinerHelper( "ALl", "leFt", new String[]{ "deptno" }, CombinerType.JOIN_RIGHT );
    }


    @Test
    public void testORCombinerWithNONEOnAndLEFTNullModifiers() throws InvalidModifierException {
        testORCombinerHelper( "NoNe", "lEft", new String[0], CombinerType.JOIN_RIGHT );
    }


    @Test
    public void testORCombinerWithRIGHTNullModifier() throws InvalidModifierException {
        testORCombinerHelper( null, "riGht", new String[0], CombinerType.JOIN_LEFT );
    }


    @Test
    public void testORCombinerWithALLOnAndRIGHTNullModifiers() throws InvalidModifierException {
        testORCombinerHelper( "ALl", "RigHt", new String[]{ "deptno" }, CombinerType.JOIN_LEFT );
    }


    @Test
    public void testORCombinerWithNONEOnAndRIGHTNullModifiers() throws InvalidModifierException {
        testORCombinerHelper( "NoNe", "rIghT", new String[0], CombinerType.JOIN_LEFT );
    }


    @Test
    public void testORCombinerWithBOTHNullModifier() throws InvalidModifierException {
        testORCombinerHelper( null, "boTh", new String[0], CombinerType.JOIN_FULL );
    }


    @Test
    public void testORCombinerWithALLOnAndBOTHNullModifiers() throws InvalidModifierException {
        testORCombinerHelper( "ALl", "boTh", new String[]{ "deptno" }, CombinerType.JOIN_FULL );
    }


    @Test
    public void testORCombinerWithNONEOnAndBOTHNullModifiers() throws InvalidModifierException {
        testORCombinerHelper( "NoNe", "BotH", new String[0], CombinerType.JOIN_FULL );
    }


    private void testANDCombinerHelper( boolean withModifiers, String onModifierValue, String[] expectedJoinOnColumns ) throws InvalidModifierException {
        Map<String, Modifier> modifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        if ( withModifiers ) {
            modifiers.put( "on", new Modifier( "on", Comparator.EQUALS, onModifierValue ) );
        }
        Combiner combiner = Combiner.createCombiner(
                new BooleanGroup<>( EntityOpsBooleanOperator.AND, modifiers ),
                employee,
                dept
        );
        algBuilder = combiner.combine( algBuilder, rexBuilder );
        AlgNode algNode = algBuilder.peek();
        List<String> actualFieldNames = algNode.getTupleType().getFieldNames();
        List<String> expectedFieldNames = new ArrayList<>();
        expectedFieldNames.add( "empno" );
        expectedFieldNames.add( "empname" );
        expectedFieldNames.add( "salary" );
        expectedFieldNames.add( "deptno" );
        expectedFieldNames.add( "married" );
        expectedFieldNames.add( "dob" );
        expectedFieldNames.add( "joining_date" );
        expectedFieldNames.add( "deptno0" );
        expectedFieldNames.add( "deptname" );

        assertEquals( CombinerType.JOIN_INNER, combiner.combinerType );
        assertArrayEquals( expectedJoinOnColumns, combiner.joinOnColumns );
        assertEquals( expectedFieldNames, actualFieldNames );
    }


    private void testORCombinerHelper( String onModifierValue, String nullModifierValue, String[] expectedJoinOnColumns, CombinerType expectedCombinerType ) throws InvalidModifierException {

        Map<String, Modifier> modifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        if ( onModifierValue != null ) {
            modifiers.put( "on", new Modifier( "on", Comparator.EQUALS, onModifierValue ) );
        }
        if ( nullModifierValue != null ) {
            modifiers.put( "null", new Modifier( "null", Comparator.EQUALS, nullModifierValue ) );
        }
        Combiner combiner = Combiner.createCombiner(
                new BooleanGroup<>( EntityOpsBooleanOperator.OR, modifiers ),
                employee,
                dept
        );
        algBuilder = combiner.combine( algBuilder, rexBuilder );
        AlgNode algNode = algBuilder.peek();
        List<String> actualFieldNames = algNode.getTupleType().getFieldNames();
        List<String> expectedFieldNames = new ArrayList<>();
        expectedFieldNames.add( "empno" );
        expectedFieldNames.add( "empname" );
        expectedFieldNames.add( "salary" );
        expectedFieldNames.add( "deptno" );
        expectedFieldNames.add( "married" );
        expectedFieldNames.add( "dob" );
        expectedFieldNames.add( "joining_date" );
        expectedFieldNames.add( "deptno0" );
        expectedFieldNames.add( "deptname" );

        assertEquals( expectedCombinerType, combiner.combinerType );
        assertArrayEquals( expectedJoinOnColumns, combiner.joinOnColumns );
        assertEquals( expectedFieldNames, actualFieldNames );
    }

}
