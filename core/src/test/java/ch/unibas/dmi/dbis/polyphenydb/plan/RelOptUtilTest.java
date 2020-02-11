/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScottSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.SqlParserConfig;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.util.TestUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;


/**
 * Unit test for {@link RelOptUtil} and other classes in this package.
 */
public class RelOptUtilTest {

    /**
     * Creates a config based on the "scott" schema.
     */
    private static Frameworks.ConfigBuilder config() {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "scott", new ReflectiveSchema( new ScottSchema() ) );

        return Frameworks.newConfigBuilder()
                .parserConfig( SqlParserConfig.DEFAULT )
                .defaultSchema( schema )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( schema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) );
    }


    private static final RelBuilder REL_BUILDER = RelBuilder.create( config().build() );
    private static final RelNode EMP_SCAN = REL_BUILDER.scan( "EMP" ).build();
    private static final RelNode DEPT_SCAN = REL_BUILDER.scan( "DEPT" ).build();

    private static final RelDataType EMP_ROW = EMP_SCAN.getRowType();
    private static final RelDataType DEPT_ROW = DEPT_SCAN.getRowType();

    private static final List<RelDataTypeField> EMP_DEPT_JOIN_REL_FIELDS = Lists.newArrayList( Iterables.concat( EMP_ROW.getFieldList(), DEPT_ROW.getFieldList() ) );


    public RelOptUtilTest() {
    }


    @Test
    public void testTypeDump() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        RelDataType t1 = typeFactory.builder()
                .add( "f0", null, SqlTypeName.DECIMAL, 5, 2 )
                .add( "f1", null, SqlTypeName.VARCHAR, 10 )
                .build();
        TestUtil.assertEqualsVerbose( TestUtil.fold( "f0 DECIMAL(5, 2) NOT NULL,", "f1 VARCHAR(10) NOT NULL" ), Util.toLinux( RelOptUtil.dumpType( t1 ) + "\n" ) );

        RelDataType t2 = typeFactory.builder()
                .add( "f0", null, t1 )
                .add( "f1", null, typeFactory.createMultisetType( t1, -1 ) )
                .build();
        TestUtil.assertEqualsVerbose(
                TestUtil.fold(
                        "f0 RECORD (",
                        "  f0 DECIMAL(5, 2) NOT NULL,",
                        "  f1 VARCHAR(10) NOT NULL) NOT NULL,",
                        "f1 RECORD (",
                        "  f0 DECIMAL(5, 2) NOT NULL,",
                        "  f1 VARCHAR(10) NOT NULL) NOT NULL MULTISET NOT NULL" ),
                Util.toLinux( RelOptUtil.dumpType( t2 ) + "\n" ) );
    }


    /**
     * Tests the rules for how we name rules.
     */
    @Test
    public void testRuleGuessDescription() {
        assertEquals( "Bar", RelOptRule.guessDescription( "com.foo.Bar" ) );
        assertEquals( "Baz", RelOptRule.guessDescription( "com.flatten.Bar$Baz" ) );

        // yields "1" (which as an integer is an invalid
        try {
            Util.discard( RelOptRule.guessDescription( "com.foo.Bar$1" ) );
            fail( "expected exception" );
        } catch ( RuntimeException e ) {
            assertEquals( "Derived description of rule class com.foo.Bar$1 is an integer, not valid. Supply a description manually.", e.getMessage() );
        }
    }


    /**
     * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)} where the join condition contains just one which is a EQUAL operator.
     */
    @Test
    public void testSplitJoinConditionEquals() {
        int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf( "deptno" );
        int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf( "deptno" );

        RexNode joinCond = REL_BUILDER.call( SqlStdOperatorTable.EQUALS, RexInputRef.of( leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS ), RexInputRef.of( EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS ) );

        splitJoinConditionHelper(
                joinCond,
                Collections.singletonList( leftJoinIndex ),
                Collections.singletonList( rightJoinIndex ),
                Collections.singletonList( true ),
                REL_BUILDER.literal( true ) );
    }


    /**
     * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)} where the join condition contains just one which is a IS NOT DISTINCT operator.
     */
    @Test
    public void testSplitJoinConditionIsNotDistinctFrom() {
        int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf( "deptno" );
        int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf( "deptno" );

        RexNode joinCond = REL_BUILDER.call( SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, RexInputRef.of( leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS ), RexInputRef.of( EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS ) );

        splitJoinConditionHelper( joinCond, Collections.singletonList( leftJoinIndex ), Collections.singletonList( rightJoinIndex ), Collections.singletonList( false ), REL_BUILDER.literal( true ) );
    }


    /**
     * Test {@link RelOptUtil#splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)} where the join condition contains an expanded version of IS NOT DISTINCT
     */
    @Test
    public void testSplitJoinConditionExpandedIsNotDistinctFrom() {
        int leftJoinIndex = EMP_SCAN.getRowType().getFieldNames().indexOf( "deptno" );
        int rightJoinIndex = DEPT_ROW.getFieldNames().indexOf( "deptno" );

        RexInputRef leftKeyInputRef = RexInputRef.of( leftJoinIndex, EMP_DEPT_JOIN_REL_FIELDS );
        RexInputRef rightKeyInputRef = RexInputRef.of( EMP_ROW.getFieldCount() + rightJoinIndex, EMP_DEPT_JOIN_REL_FIELDS );
        RexNode joinCond = REL_BUILDER.call(
                SqlStdOperatorTable.OR,
                REL_BUILDER.call( SqlStdOperatorTable.EQUALS, leftKeyInputRef, rightKeyInputRef ),
                REL_BUILDER.call( SqlStdOperatorTable.AND, REL_BUILDER.call( SqlStdOperatorTable.IS_NULL, leftKeyInputRef ), REL_BUILDER.call( SqlStdOperatorTable.IS_NULL, rightKeyInputRef ) ) );

        splitJoinConditionHelper( joinCond, Collections.singletonList( leftJoinIndex ), Collections.singletonList( rightJoinIndex ), Collections.singletonList( false ), REL_BUILDER.literal( true ) );
    }


    private static void splitJoinConditionHelper( RexNode joinCond, List<Integer> expLeftKeys, List<Integer> expRightKeys, List<Boolean> expFilterNulls, RexNode expRemaining ) {
        List<Integer> actLeftKeys = new ArrayList<>();
        List<Integer> actRightKeys = new ArrayList<>();
        List<Boolean> actFilterNulls = new ArrayList<>();

        RexNode actRemaining = RelOptUtil.splitJoinCondition( EMP_SCAN, DEPT_SCAN, joinCond, actLeftKeys, actRightKeys, actFilterNulls );

        assertEquals( expRemaining.toString(), actRemaining.toString() );
        assertEquals( expFilterNulls, actFilterNulls );
        assertEquals( expLeftKeys, actLeftKeys );
        assertEquals( expRightKeys, actRightKeys );
    }
}

