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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.plan;


import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.polypheny.db.plan.AlgOptRule.operand;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.adapter.jdbc.JdbcAlg;
import org.polypheny.db.adapter.jdbc.JdbcConvention;
import org.polypheny.db.adapter.jdbc.JdbcImplementor;
import org.polypheny.db.adapter.jdbc.JdbcRules;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Lex;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.ChainedOperatorTable;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.rules.FilterMergeRule;
import org.polypheny.db.algebra.rules.LoptOptimizeJoinRule;
import org.polypheny.db.algebra.rules.ProjectMergeRule;
import org.polypheny.db.algebra.rules.ProjectToWindowRules;
import org.polypheny.db.algebra.rules.SortJoinTransposeRule;
import org.polypheny.db.algebra.rules.SortProjectTransposeRule;
import org.polypheny.db.algebra.rules.SortRemoveRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.FoodmartSchema;
import org.polypheny.db.schema.HrSchema;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TpchSchema;
import org.polypheny.db.sql.SqlLanguagelDependant;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.util.ListSqlOperatorTable;
import org.polypheny.db.test.PolyphenyDbAssert;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgConversionException;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RuleSet;
import org.polypheny.db.tools.RuleSets;
import org.polypheny.db.tools.ValidationException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.Util;


/**
 * Unit tests for {@link Planner}.
 */
public class PlannerTest extends SqlLanguagelDependant {

    static {
        Catalog.testMode = true;
    }


    private void checkParseAndConvert( String query, String queryFromParseTree, String expectedRelExpr ) throws Exception {
        Planner planner = getPlanner( null );
        Node parse = planner.parse( query );
        assertThat( Util.toLinux( parse.toString() ), equalTo( queryFromParseTree ) );

        Node validate = planner.validate( parse );
        AlgNode alg = planner.alg( validate ).project();
        assertThat( toString( alg ), equalTo( expectedRelExpr ) );
    }


    @Test
    public void testParseAndConvert() throws Exception {
        checkParseAndConvert(
                "select * from \"emps\" where \"name\" like '%e%'",

                "SELECT *\n"
                        + "FROM `emps`\n"
                        + "WHERE `name` LIKE '%e%'",

                "LogicalProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
                        + "  LogicalFilter(model=[RELATIONAL], condition=[LIKE($2, '%e%')])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    @Test(expected = NodeParseException.class)
    public void testParseIdentiferMaxLengthWithDefault() throws Exception {
        Planner planner = getPlanner( null, Parser.configBuilder().build() );
        planner.parse( "select name as " + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from \"emps\"" );
    }


    @Test
    public void testParseIdentiferMaxLengthWithIncreased() throws Exception {
        Planner planner = getPlanner( null, Parser.configBuilder().setIdentifierMaxLength( 512 ).build() );
        planner.parse( "select name as " + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from \"emps\"" );
    }


    /**
     * Unit test that parses, validates and converts the query using order by and offset.
     */
    @Test
    public void testParseAndConvertWithOrderByAndOffset() throws Exception {
        checkParseAndConvert(
                "select * from \"emps\" order by \"emps\".\"deptno\" offset 10",

                "SELECT *\n"
                        + "FROM `emps`\n"
                        + "ORDER BY `emps`.`deptno`\n"
                        + "OFFSET 10 ROWS",

                "LogicalSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC], offset=[10])\n"
                        + "  LogicalProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    private String toString( AlgNode alg ) {
        return Util.toLinux( AlgOptUtil.dumpPlan( "", alg, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
    }


    @Test
    public void testParseFails() throws NodeParseException {
        Planner planner = getPlanner( null );
        try {
            Node parse = planner.parse( "select * * from \"emps\"" );
            fail( "expected error, got " + parse );
        } catch ( NodeParseException e ) {
            assertThat( e.getMessage(), containsString( "Encountered \"*\" at line 1, column 10." ) );
        }
    }


    @Test
    public void testValidateFails() throws NodeParseException {
        Planner planner = getPlanner( null );
        Node parse = planner.parse( "select * from \"emps\" where \"Xname\" like '%e%'" );
        assertThat(
                Util.toLinux( parse.toString() ),
                equalTo( "SELECT *\n"
                        + "FROM `emps`\n"
                        + "WHERE `Xname` LIKE '%e%'" ) );

        try {
            Node validate = planner.validate( parse );
            fail( "expected error, got " + validate );
        } catch ( ValidationException e ) {
            assertThat( Throwables.getStackTraceAsString( e ), containsString( "Column 'Xname' not found in any table" ) );
            // ok
        }
    }


    @Test
    public void testValidateUserDefinedAggregate() throws Exception {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );

        final SqlStdOperatorTable stdOpTab = SqlStdOperatorTable.instance();
        OperatorTable opTab = ChainedOperatorTable.of( stdOpTab, new ListSqlOperatorTable( ImmutableList.of( new MyCountAggFunction() ) ) );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema( schema )
                .operatorTable( opTab )
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
                        null ) )
                .build();
        final Planner planner = Frameworks.getPlanner( config );
        Node parse =
                planner.parse( "select \"deptno\", my_count(\"empid\") from \"emps\"\n"
                        + "group by \"deptno\"" );
        assertThat(
                Util.toLinux( parse.toString() ),
                equalTo( "SELECT `deptno`, `MY_COUNT`(`empid`)\n"
                        + "FROM `emps`\n"
                        + "GROUP BY `deptno`" ) );

        // MY_COUNT is recognized as an aggregate function, and therefore it is OK that its argument empid is not in the GROUP BY clause.
        Node validate = planner.validate( parse );
        assertThat( validate, notNullValue() );

        // The presence of an aggregate function in the SELECT clause causes it to become an aggregate query. Non-aggregate expressions become illegal.
        planner.close();
        planner.reset();
        parse = planner.parse( "select \"deptno\", count(1) from \"emps\"" );
        try {
            validate = planner.validate( parse );
            fail( "expected exception, got " + validate );
        } catch ( ValidationException e ) {
            assertThat( e.getCause().getCause().getMessage(), containsString( "Expression 'deptno' is not being grouped" ) );
        }
    }


    private Planner getPlanner( List<AlgTraitDef> traitDefs, Program... programs ) {
        return getPlanner( traitDefs, Parser.ParserConfig.DEFAULT, programs );
    }


    private Planner getPlanner( List<AlgTraitDef> traitDefs, ParserConfig parserConfig, Program... programs ) {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .defaultSchema( schema )
                .traitDefs( traitDefs )
                .programs( programs )
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
                        null ) )
                .build();
        return Frameworks.getPlanner( config );
    }


    /**
     * Tests that planner throws an error if you pass to {@link Planner#alg(Node)} a {@link SqlNode} that has been parsed but not validated.
     */
    @Test
    public void testConvertWithoutValidateFails() throws Exception {
        Planner planner = getPlanner( null );
        Node parse = planner.parse( "select * from \"emps\"" );
        try {
            AlgRoot alg = planner.alg( parse );
            fail( "expected error, got " + alg );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), containsString( "cannot move from STATE_3_PARSED to STATE_4_VALIDATED" ) );
        }
    }


    /**
     * Helper method for testing {@link AlgMetadataQuery#getPulledUpPredicates} metadata.
     */
    private void checkMetadataPredicates( String sql, String expectedPredicates ) throws Exception {
        Planner planner = getPlanner( null );
        Node parse = planner.parse( sql );
        Node validate = planner.validate( parse );
        AlgNode alg = planner.alg( validate ).project();
        final AlgMetadataQuery mq = AlgMetadataQuery.instance();
        final AlgOptPredicateList predicates = mq.getPulledUpPredicates( alg );
        final String buf = predicates.pulledUpPredicates.toString();
        assertThat( buf, equalTo( expectedPredicates ) );
    }


    /**
     * Tests predicates that can be pulled-up from a UNION.
     */
    @Test
    public void testMetadataUnionPredicates() throws Exception {
        checkMetadataPredicates(
                "select * from \"emps\" where \"deptno\" < 10\n" + "union all\n" + "select * from \"emps\" where \"empid\" > 2",
                "[OR(<($1, 10), >($0, 2))]" );
    }


    /**
     * Test case for "getPredicates from a union is not correct".
     */
    @Test
    public void testMetadataUnionPredicates2() throws Exception {
        checkMetadataPredicates(
                "select * from \"emps\" where \"deptno\" < 10\n" + "union all\n" + "select * from \"emps\"",
                "[]" );
    }


    @Test
    public void testMetadataUnionPredicates3() throws Exception {
        checkMetadataPredicates(
                "select * from \"emps\" where \"deptno\" < 10\n" + "union all\n" + "select * from \"emps\" where \"deptno\" < 10 and \"empid\" > 1",
                "[<($1, 10)]" );
    }


    @Test
    public void testMetadataUnionPredicates4() throws Exception {
        checkMetadataPredicates(
                "select * from \"emps\" where \"deptno\" < 10\n" + "union all\n" + "select * from \"emps\" where \"deptno\" < 10 or \"empid\" > 1",
                "[OR(<($1, 10), >($0, 1))]" );
    }


    @Test
    public void testMetadataUnionPredicates5() throws Exception {
        final String sql = "select * from \"emps\" where \"deptno\" < 10\n" + "union all\n" + "select * from \"emps\" where \"deptno\" < 10 and false";
        checkMetadataPredicates( sql, "[<($1, 10)]" );
    }


    /**
     * Tests predicates that can be pulled-up from an Aggregate with {@code GROUP BY ()}. This form of Aggregate can convert an empty relation
     * to a single-row relation, so it is not valid to pull up the predicate {@code false}.
     */
    @Test
    public void testMetadataAggregatePredicates() throws Exception {
        checkMetadataPredicates( "select count(*) from \"emps\" where false", "[]" );
    }


    /**
     * Tests predicates that can be pulled-up from an Aggregate with a non-empty group key. The {@code false} predicate effectively means that the relation is empty, because no row can satisfy {@code false}.
     */
    @Test
    public void testMetadataAggregatePredicates2() throws Exception {
        final String sql = "select \"deptno\", count(\"deptno\")\n" + "from \"emps\" where false\n" + "group by \"deptno\"";
        checkMetadataPredicates( sql, "[false]" );
    }


    @Test
    public void testMetadataAggregatePredicates3() throws Exception {
        final String sql = "select \"deptno\", count(\"deptno\")\n" + "from \"emps\" where \"deptno\" > 10\n" + "group by \"deptno\"";
        checkMetadataPredicates( sql, "[>($0, 10)]" );
    }


    /**
     * Unit test that parses, validates, converts and plans.
     */
    @Test
    public void testPlan() throws Exception {
        Program program = Programs.ofRules( FilterMergeRule.INSTANCE, EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_PROJECT_RULE );
        Planner planner = getPlanner( null, program );
        Node parse = planner.parse( "select * from \"emps\"" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat(
                toString( transform ),
                equalTo( "EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n  EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    /**
     * Unit test that parses, validates, converts and plans for query using order by
     */
    @Test
    public void testSortPlan() throws Exception {
        RuleSet ruleSet = RuleSets.ofList( SortRemoveRule.INSTANCE, EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_SORT_RULE );
        Planner planner = getPlanner( null, Programs.of( ruleSet ) );
        Node parse = planner.parse( "select * from \"emps\" order by \"emps\".\"deptno\"" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat(
                toString( transform ),
                equalTo( "EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    /**
     * Test case for "Enrich EnumerableJoin operator with order preserving information".
     *
     * Since the left input to the join is sorted, and this join preserves order, there shouldn't be any sort operator above the join.
     */
    @Test
    public void testRedundantSortOnJoinPlan() throws Exception {
        RuleSet ruleSet =
                RuleSets.ofList(
                        SortRemoveRule.INSTANCE,
                        SortJoinTransposeRule.INSTANCE,
                        SortProjectTransposeRule.INSTANCE,
                        EnumerableRules.ENUMERABLE_LIMIT_RULE,
                        EnumerableRules.ENUMERABLE_JOIN_RULE,
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_SORT_RULE );
        Planner planner = getPlanner( null, Programs.of( ruleSet ) );
        Node parse = planner.parse( "select e.\"deptno\" from \"emps\" e left outer join \"depts\" d on e.\"deptno\" = d.\"deptno\" order by e.\"deptno\" limit 10" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE ).simplify();
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat(
                toString( transform ),
                equalTo( "EnumerableProject(model=[RELATIONAL], deptno=[$1])\n"
                        + "  EnumerableLimit(model=[RELATIONAL], fetch=[10])\n"
                        + "    EnumerableJoin(model=[RELATIONAL], condition=[=($1, $5)], joinType=[left])\n"
                        + "      EnumerableLimit(model=[RELATIONAL], fetch=[10])\n"
                        + "        EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "          EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n"
                        + "      EnumerableProject(model=[RELATIONAL], deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
                        + "        EnumerableScan(model=[RELATIONAL], table=[[hr, depts]])\n" ) );
    }


    /**
     * Unit test that parses, validates, converts and plans for query using two duplicate order by.
     * The duplicate order by should be removed by SqlToAlgConverter.
     */
    @Test
    public void testDuplicateSortPlan() throws Exception {
        runDuplicateSortCheck(
                "select empid from ( select * from emps order by emps.deptno) order by deptno",
                "EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    /**
     * Unit test that parses, validates, converts and plans for query using two duplicate order by.
     * The duplicate order by should be removed by SqlToAlgConverter.
     */
    @Test
    public void testDuplicateSortPlanWithExpr() throws Exception {
        runDuplicateSortCheck(
                "select empid+deptno from ( select empid, deptno from emps order by emps.deptno) order by deptno",
                "EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], EXPR$0=[+($0, $1)], deptno=[$1])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    @Test
    public void testTwoSortRemoveInnerSort() throws Exception {
        runDuplicateSortCheck(
                "select empid+deptno from ( select empid, deptno from emps order by empid) order by deptno",
                "EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], EXPR$0=[+($0, $1)], deptno=[$1])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    /**
     * Tests that outer order by is not removed since window function might reorder the rows in-between
     */
    @Test
    public void testDuplicateSortPlanWithOver() throws Exception {
        runDuplicateSortCheck(
                "select emp_cnt, empid+deptno from ( "
                        + "select empid, deptno, count(*) over (partition by deptno) emp_cnt from ( "
                        + "  select empid, deptno "
                        + "    from emps "
                        + "   order by emps.deptno) "
                        + ")"
                        + "order by deptno",
                "EnumerableSort(model=[RELATIONAL], sort0=[$2], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], emp_cnt=[$5], EXPR$1=[+($0, $1)], deptno=[$1])\n"
                        + "    EnumerableWindow(model=[RELATIONAL], window#0=[window(partition {1} order by [] range between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING aggs [COUNT()])])\n"
                        + "      EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    @Test
    public void testDuplicateSortPlanWithRemovedOver() throws Exception {
        runDuplicateSortCheck(
                "select empid+deptno from ( "
                        + "select empid, deptno, count(*) over (partition by deptno) emp_cnt from ( "
                        + "  select empid, deptno "
                        + "    from emps "
                        + "   order by emps.deptno) "
                        + ")"
                        + "order by deptno",
                "EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], EXPR$0=[+($0, $1)], deptno=[$1])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" );
    }


    // If proper "SqlParseException, ValidationException, AlgConversionException" is used, then checkstyle fails with
    // "Redundant throws: 'ValidationException' listed more then one time"
    // "Redundant throws: 'AlgConversionException' listed more then one time"
    private void runDuplicateSortCheck( String sql, String plan ) throws Exception {
        RuleSet ruleSet = RuleSets.ofList( SortRemoveRule.INSTANCE, EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_WINDOW_RULE, EnumerableRules.ENUMERABLE_SORT_RULE, ProjectToWindowRules.PROJECT );
        Planner planner = getPlanner( null, Parser.configBuilder().setLex( Lex.JAVA ).build(), Programs.of( ruleSet ) );
        Node parse = planner.parse( sql );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        if ( traitSet.getTrait( AlgCollationTraitDef.INSTANCE ) == null ) {
            // SortRemoveRule can only work if collation trait is enabled.
            return;
        }
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat( toString( transform ), equalTo( plan ) );
    }


    /**
     * Unit test that parses, validates, converts and plans for query using two duplicate order by.
     */
    @Test
    public void testDuplicateSortPlanWORemoveSortRule() throws Exception {
        RuleSet ruleSet = RuleSets.ofList( EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_SORT_RULE );
        Planner planner = getPlanner( null, Programs.of( ruleSet ) );
        Node parse = planner.parse(
                "select \"empid\" from ( "
                        + "select * "
                        + "from \"emps\" "
                        + "order by \"emps\".\"deptno\") "
                        + "order by \"deptno\"" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat(
                toString( transform ),
                equalTo( "EnumerableSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC])\n"
                        + "  EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1])\n"
                        + "    EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    /**
     * Unit test that parses, validates, converts and plans. Planner is provided with a list of RelTraitDefs to register.
     */
    @Test
    public void testPlanWithExplicitTraitDefs() throws Exception {
        RuleSet ruleSet = RuleSets.ofList( FilterMergeRule.INSTANCE, EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_PROJECT_RULE );
        final List<AlgTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        traitDefs.add( AlgCollationTraitDef.INSTANCE );

        Planner planner = getPlanner( traitDefs, Programs.of( ruleSet ) );

        Node parse = planner.parse( "select * from \"emps\"" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat(
                toString( transform ),
                equalTo( "EnumerableProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n  EnumerableScan(table=[[hr, emps]])\n" ) );
    }


    /**
     * Unit test that calls {@link Planner#transform} twice.
     */
    @Test
    public void testPlanTransformTwice() throws Exception {
        RuleSet ruleSet = RuleSets.ofList( FilterMergeRule.INSTANCE, EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_PROJECT_RULE );
        Planner planner = getPlanner( null, Programs.of( ruleSet ) );
        Node parse = planner.parse( "select * from \"emps\"" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        AlgNode transform2 = planner.transform( 0, traitSet, transform );
        assertThat(
                toString( transform2 ),
                equalTo( "EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n  EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    /**
     * Unit test that calls {@link Planner#transform} twice with rule name conflicts
     */
    @Test
    public void testPlanTransformWithRuleNameConflicts() throws Exception {
        // Create two dummy rules with identical rules.
        AlgOptRule rule1 = new AlgOptRule( operand( LogicalProject.class, operand( LogicalFilter.class, AlgOptRule.any() ) ), "MYRULE" ) {
            @Override
            public boolean matches( AlgOptRuleCall call ) {
                return false;
            }


            @Override
            public void onMatch( AlgOptRuleCall call ) {
            }
        };

        AlgOptRule rule2 = new AlgOptRule( operand( LogicalFilter.class, operand( LogicalProject.class, AlgOptRule.any() ) ), "MYRULE" ) {
            @Override
            public boolean matches( AlgOptRuleCall call ) {
                return false;
            }


            @Override
            public void onMatch( AlgOptRuleCall call ) {
            }
        };

        RuleSet ruleSet1 = RuleSets.ofList( rule1, EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_PROJECT_RULE );

        RuleSet ruleSet2 = RuleSets.ofList( rule2 );

        Planner planner = getPlanner( null, Programs.of( ruleSet1 ), Programs.of( ruleSet2 ) );
        Node parse = planner.parse( "select * from \"emps\"" );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        AlgNode transform2 = planner.transform( 1, traitSet, transform );
        assertThat(
                toString( transform2 ),
                equalTo( "EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n  EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    /**
     * Tests that Hive dialect does not generate "AS".
     */
    @Test
    public void testHiveDialect() throws NodeParseException {
        Planner planner = getPlanner( null );
        Node parse = planner.parse( "select * from (select * from \"emps\") as t\n" + "where \"name\" like '%e%'" );
        final SqlDialect hiveDialect = SqlDialect.DatabaseProduct.HIVE.getDialect();
        assertThat(
                Util.toLinux( ((SqlNode) parse).toSqlString( hiveDialect ).getSql() ),
                equalTo( "SELECT *\n" + "FROM (SELECT *\n" + "FROM emps) T\n" + "WHERE name LIKE '%e%'" ) );
    }


    /**
     * Unit test that calls {@link Planner#transform} twice, with different rule sets, with different conventions.
     *
     * {@link JdbcConvention} is different from the typical convention in that it is not a singleton. Switching to a different
     * instance causes problems unless planner state is wiped clean between calls to {@link Planner#transform}.
     */
    @Test
    public void testPlanTransformWithDiffRuleSetAndConvention() throws Exception {
        Program program0 = Programs.ofRules( FilterMergeRule.INSTANCE, EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_PROJECT_RULE );

        JdbcConvention out = new JdbcConvention( null, null, "myjdbc" );
        Program program1 = Programs.ofRules( new MockJdbcProjectRule( out ), new MockJdbcTableRule( out ) );

        Planner planner = getPlanner( null, program0, program1 );
        Node parse = planner.parse( "select T1.\"name\" from \"emps\" as T1 " );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();

        AlgTraitSet traitSet0 = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );

        AlgTraitSet traitSet1 = convert.getTraitSet().replace( out );

        AlgNode transform = planner.transform( 0, traitSet0, convert );
        AlgNode transform2 = planner.transform( 1, traitSet1, transform );
        assertThat(
                toString( transform2 ),
                equalTo( "JdbcProject(model=[RELATIONAL], name=[$2])\n  MockJdbcScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    /**
     * Unit test that plans a query with a large number of joins.
     */
    @Test
    public void testPlanNWayJoin() throws Exception {
        // Here the times before and after enabling LoptOptimizeJoinRule.
        //
        // Note the jump between N=6 and N=7; LoptOptimizeJoinRule is disabled if
        // there are fewer than 6 joins (7 relations).
        //
        //       N    Before     After
        //         time (ms) time (ms)
        // ======= ========= =========
        //       5                 382
        //       6                 790
        //       7                  26
        //       9    6,000         39
        //      10    9,000         47
        //      11   19,000         67
        //      12   40,000         63
        //      13 OOM              96
        //      35 OOM           1,716
        //      60 OOM          12,230
        checkJoinNWay( 5 ); // LoptOptimizeJoinRule disabled; takes about .4s
        checkJoinNWay( 9 ); // LoptOptimizeJoinRule enabled; takes about 0.04s
        checkJoinNWay( 35 ); // takes about 2s
        if ( PolyphenyDbAssert.ENABLE_SLOW ) {
            checkJoinNWay( 60 ); // takes about 15s
        }
    }


    private void checkJoinNWay( int n ) throws Exception {
        final StringBuilder buf = new StringBuilder();
        buf.append( "select *" );
        for ( int i = 0; i < n; i++ ) {
            buf.append( i == 0 ? "\nfrom " : ",\n " )
                    .append( "\"depts\" as d" )
                    .append( i );
        }
        for ( int i = 1; i < n; i++ ) {
            buf.append( i == 1 ? "\nwhere" : "\nand" )
                    .append( " d" )
                    .append( i ).append( ".\"deptno\" = d" )
                    .append( i - 1 ).append( ".\"deptno\"" );
        }
        Planner planner = getPlanner( null, Programs.heuristicJoinOrder( Programs.RULE_SET, false, 6 ) );
        Node parse = planner.parse( buf.toString() );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat(
                toString( transform ),
                containsString( "EnumerableJoin(model=[RELATIONAL], condition=[=($0, $5)], joinType=[inner])" ) );
    }


    /**
     * Test case for "LoptOptimizeJoinRule incorrectly re-orders outer joins".
     *
     * Checks the {@link LoptOptimizeJoinRule} on a query with a left outer join.
     *
     * Specifically, tests that a relation (dependents) in an inner join cannot be pushed into an outer join (emps left join depts).
     */
    @Test
    public void testHeuristicLeftJoin() throws Exception {
        final String sql = "select * from \"emps\" as e\n"
                + "left join \"depts\" as d on e.\"deptno\" = d.\"deptno\"\n"
                + "join \"dependents\" as p on e.\"empid\" = p.\"empid\"";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4], deptno0=[$5], name0=[$6], employees=[$7], location=[$8], location9=[$9], empid0=[$10], name1=[$11])\n"
                + "  EnumerableProject(model=[RELATIONAL], empid=[$2], deptno=[$3], name=[$4], salary=[$5], commission=[$6], deptno0=[$7], name0=[$8], employees=[$9], x=[$10], y=[$11], empid0=[$0], name1=[$1])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[=($0, $2)], joinType=[inner])\n"
                + "      EnumerableScan(model=[RELATIONAL], table=[[hr, dependents]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($1, $5)], joinType=[left])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n"
                + "        EnumerableProject(model=[RELATIONAL], deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[hr, depts]])";
        checkHeuristic( sql, expected );
    }


    /**
     * It would probably be OK to transform {@code (emps right join depts) join dependents} to {@code (emps  join dependents) right join depts} but we do not currently allow it.
     */
    @Test
    public void testHeuristicPushInnerJoin() throws Exception {
        final String sql = "select * from \"emps\" as e\n"
                + "right join \"depts\" as d on e.\"deptno\" = d.\"deptno\"\n"
                + "join \"dependents\" as p on e.\"empid\" = p.\"empid\"";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4], deptno0=[$5], name0=[$6], employees=[$7], location=[$8], location9=[$9], empid0=[$10], name1=[$11])\n"
                + "  EnumerableProject(model=[RELATIONAL], empid=[$2], deptno=[$3], name=[$4], salary=[$5], commission=[$6], deptno0=[$7], name0=[$8], employees=[$9], x=[$10], y=[$11], empid0=[$0], name1=[$1])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[=($0, $2)], joinType=[inner])\n"
                + "      EnumerableScan(model=[RELATIONAL], table=[[hr, dependents]])\n"
                + "      EnumerableProject(model=[RELATIONAL], empid=[$5], deptno=[$6], name=[$7], salary=[$8], commission=[$9], deptno0=[$0], name0=[$1], employees=[$2], x=[$3], y=[$4])\n"
                + "        EnumerableJoin(model=[RELATIONAL], condition=[=($0, $6)], joinType=[left])\n"
                + "          EnumerableProject(model=[RELATIONAL], deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
                + "            EnumerableScan(model=[RELATIONAL], table=[[hr, depts]])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])";
        checkHeuristic( sql, expected );
    }


    /**
     * Tests that a relation (dependents) that is on the null-generating side of an outer join cannot be pushed into an inner join (emps join depts).
     */
    @Test
    public void testHeuristicRightJoin() throws Exception {
        final String sql = "select * from \"emps\" as e\n"
                + "join \"depts\" as d on e.\"deptno\" = d.\"deptno\"\n"
                + "right join \"dependents\" as p on e.\"empid\" = p.\"empid\"";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4], deptno0=[$5], name0=[$6], employees=[$7], location=[$8], location9=[$9], empid0=[$10], name1=[$11])\n"
                + "  EnumerableProject(model=[RELATIONAL], empid=[$2], deptno=[$3], name=[$4], salary=[$5], commission=[$6], deptno0=[$7], name0=[$8], employees=[$9], x=[$10], y=[$11], empid0=[$0], name1=[$1])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[=($0, $2)], joinType=[left])\n"
                + "      EnumerableScan(model=[RELATIONAL], table=[[hr, dependents]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($1, $5)], joinType=[inner])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[hr, emps]])\n"
                + "        EnumerableProject(model=[RELATIONAL], deptno=[$0], name=[$1], employees=[$2], x=[$3.x], y=[$3.y])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[hr, depts]])";
        checkHeuristic( sql, expected );
    }


    private void checkHeuristic( String sql, String expected ) throws Exception {
        Planner planner = getPlanner( null, Programs.heuristicJoinOrder( Programs.RULE_SET, false, 0 ) );
        Node parse = planner.parse( sql );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat( toString( transform ), containsString( expected ) );
    }


    /**
     * Plans a 3-table join query on the FoodMart schema. The ideal plan is not bushy, but nevertheless exercises the bushy-join heuristic optimizer.
     */
    @Test
    public void testAlmostBushy() throws Exception {
        final String sql = "select *\n"
                + "from \"sales_fact_1997\" as s\n"
                + "join \"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "join \"product\" as p\n"
                + "  on s.\"product_id\" = p.\"product_id\"\n"
                + "where c.\"city\" = 'San Francisco'\n"
                + "and p.\"brand_name\" = 'Washington'";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51])\n"
                + "  EnumerableProject(model=[RELATIONAL], product_id0=[$44], time_id=[$45], customer_id0=[$46], promotion_id=[$47], store_id=[$48], store_sales=[$49], store_cost=[$50], unit_sales=[$51], customer_id=[$0], account_num=[$1], lname=[$2], fname=[$3], mi=[$4], address1=[$5], address2=[$6], address3=[$7], address4=[$8], city=[$9], state_province=[$10], postal_code=[$11], country=[$12], customer_region_id=[$13], phone1=[$14], phone2=[$15], birthdate=[$16], marital_status=[$17], yearly_income=[$18], gender=[$19], total_children=[$20], num_children_at_home=[$21], education=[$22], date_accnt_opened=[$23], member_card=[$24], occupation=[$25], houseowner=[$26], num_cars_owned=[$27], fullname=[$28], product_class_id=[$29], product_id=[$30], brand_name=[$31], product_name=[$32], SKU=[$33], SRP=[$34], gross_weight=[$35], net_weight=[$36], recyclable_package=[$37], low_fat=[$38], units_per_case=[$39], cases_per_pallet=[$40], shelf_width=[$41], shelf_height=[$42], shelf_depth=[$43])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[=($0, $46)], joinType=[inner])\n"
                + "      EnumerableFilter(model=[RELATIONAL], condition=[=(CAST($9):VARCHAR, 'San Francisco')])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, customer]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($1, $15)], joinType=[inner])\n"
                + "        EnumerableFilter(model=[RELATIONAL], condition=[=(CAST($2):VARCHAR, 'Washington')])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, product]])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, sales_fact_1997]])\n";
        checkBushy( sql, expected );
    }


    /**
     * Plans a 4-table join query on the FoodMart schema.
     *
     * The ideal plan is bushy:
     * customer x (product_class x  product x sales)
     * which would be written
     * (customer x ((product_class x product) x sales))
     * if you don't assume 'x' is left-associative.
     */
    @Test
    public void testBushy() throws Exception {
        final String sql = "select *\n"
                + "from \"sales_fact_1997\" as s\n"
                + "join \"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "join \"product\" as p\n"
                + "  on s.\"product_id\" = p.\"product_id\"\n"
                + "join \"product_class\" as pc\n"
                + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
                + "where c.\"city\" = 'San Francisco'\n"
                + "and p.\"brand_name\" = 'Washington'";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51], product_class_id0=[$52], product_subcategory=[$53], product_category=[$54], product_department=[$55], product_family=[$56])\n"
                + "  EnumerableProject(model=[RELATIONAL], product_id0=[$49], time_id=[$50], customer_id0=[$51], promotion_id=[$52], store_id=[$53], store_sales=[$54], store_cost=[$55], unit_sales=[$56], customer_id=[$20], account_num=[$21], lname=[$22], fname=[$23], mi=[$24], address1=[$25], address2=[$26], address3=[$27], address4=[$28], city=[$29], state_province=[$30], postal_code=[$31], country=[$32], customer_region_id=[$33], phone1=[$34], phone2=[$35], birthdate=[$36], marital_status=[$37], yearly_income=[$38], gender=[$39], total_children=[$40], num_children_at_home=[$41], education=[$42], date_accnt_opened=[$43], member_card=[$44], occupation=[$45], houseowner=[$46], num_cars_owned=[$47], fullname=[$48], product_class_id=[$0], product_id=[$1], brand_name=[$2], product_name=[$3], SKU=[$4], SRP=[$5], gross_weight=[$6], net_weight=[$7], recyclable_package=[$8], low_fat=[$9], units_per_case=[$10], cases_per_pallet=[$11], shelf_width=[$12], shelf_height=[$13], shelf_depth=[$14], product_class_id0=[$15], product_subcategory=[$16], product_category=[$17], product_department=[$18], product_family=[$19])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[=($1, $49)], joinType=[inner])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($0, $15)], joinType=[inner])\n"
                + "        EnumerableFilter(model=[RELATIONAL], condition=[=(CAST($2):VARCHAR, 'Washington')])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, product]])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, product_class]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($0, $31)], joinType=[inner])\n"
                + "        EnumerableFilter(model=[RELATIONAL], condition=[=(CAST($9):VARCHAR, 'San Francisco')])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, customer]])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, sales_fact_1997]])\n";
        checkBushy( sql, expected );
    }


    /**
     * Plans a 5-table join query on the FoodMart schema. The ideal plan is
     * bushy: store x (customer x (product_class x product x sales)).
     */
    @Test
    public void testBushy5() throws Exception {
        final String sql = "select *\n"
                + "from \"sales_fact_1997\" as s\n"
                + "join \"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "join \"product\" as p\n"
                + "  on s.\"product_id\" = p.\"product_id\"\n"
                + "join \"product_class\" as pc\n"
                + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
                + "join \"store\" as st\n"
                + "  on s.\"store_id\" = st.\"store_id\"\n"
                + "where c.\"city\" = 'San Francisco'\n";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], product_class_id=[$37], product_id0=[$38], brand_name=[$39], product_name=[$40], SKU=[$41], SRP=[$42], gross_weight=[$43], net_weight=[$44], recyclable_package=[$45], low_fat=[$46], units_per_case=[$47], cases_per_pallet=[$48], shelf_width=[$49], shelf_height=[$50], shelf_depth=[$51], product_class_id0=[$52], product_subcategory=[$53], product_category=[$54], product_department=[$55], product_family=[$56], store_id0=[$57], store_type=[$58], region_id=[$59], store_name=[$60], store_number=[$61], store_street_address=[$62], store_city=[$63], store_state=[$64], store_postal_code=[$65], store_country=[$66], store_manager=[$67], store_phone=[$68], store_fax=[$69], first_opened_date=[$70], last_remodel_date=[$71], store_sqft=[$72], grocery_sqft=[$73], frozen_sqft=[$74], meat_sqft=[$75], coffee_bar=[$76], video_store=[$77], salad_bar=[$78], prepared_food=[$79], florist=[$80])\n"
                + "  EnumerableProject(model=[RELATIONAL], product_id=[$29], time_id=[$30], customer_id0=[$31], promotion_id=[$32], store_id=[$33], store_sales=[$34], store_cost=[$35], unit_sales=[$36], customer_id=[$0], account_num=[$1], lname=[$2], fname=[$3], mi=[$4], address1=[$5], address2=[$6], address3=[$7], address4=[$8], city=[$9], state_province=[$10], postal_code=[$11], country=[$12], customer_region_id=[$13], phone1=[$14], phone2=[$15], birthdate=[$16], marital_status=[$17], yearly_income=[$18], gender=[$19], total_children=[$20], num_children_at_home=[$21], education=[$22], date_accnt_opened=[$23], member_card=[$24], occupation=[$25], houseowner=[$26], num_cars_owned=[$27], fullname=[$28], product_class_id=[$61], product_id0=[$62], brand_name=[$63], product_name=[$64], SKU=[$65], SRP=[$66], gross_weight=[$67], net_weight=[$68], recyclable_package=[$69], low_fat=[$70], units_per_case=[$71], cases_per_pallet=[$72], shelf_width=[$73], shelf_height=[$74], shelf_depth=[$75], product_class_id0=[$76], product_subcategory=[$77], product_category=[$78], product_department=[$79], product_family=[$80], store_id0=[$37], store_type=[$38], region_id=[$39], store_name=[$40], store_number=[$41], store_street_address=[$42], store_city=[$43], store_state=[$44], store_postal_code=[$45], store_country=[$46], store_manager=[$47], store_phone=[$48], store_fax=[$49], first_opened_date=[$50], last_remodel_date=[$51], store_sqft=[$52], grocery_sqft=[$53], frozen_sqft=[$54], meat_sqft=[$55], coffee_bar=[$56], video_store=[$57], salad_bar=[$58], prepared_food=[$59], florist=[$60])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[=($0, $31)], joinType=[inner])\n"
                + "      EnumerableFilter(model=[RELATIONAL], condition=[=(CAST($9):VARCHAR, 'San Francisco')])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, customer]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($0, $33)], joinType=[inner])\n"
                + "        EnumerableJoin(model=[RELATIONAL], condition=[=($4, $8)], joinType=[inner])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, sales_fact_1997]])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, store]])\n"
                + "        EnumerableJoin(model=[RELATIONAL], condition=[=($0, $15)], joinType=[inner])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, product]])\n"
                + "          EnumerableScan(model=[RELATIONAL], table=[[foodmart, product_class]])\n";
        checkBushy( sql, expected );
    }


    /**
     * Tests the bushy join algorithm where one table does not join to anything.
     */
    @Test
    public void testBushyCrossJoin() throws Exception {
        final String sql = "select * from \"sales_fact_1997\" as s\n"
                + "join \"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "cross join \"department\"";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], department_id=[$37], department_description=[$38])\n"
                + "  EnumerableProject(model=[RELATIONAL], product_id=[$2], time_id=[$3], customer_id=[$4], promotion_id=[$5], store_id=[$6], store_sales=[$7], store_cost=[$8], unit_sales=[$9], customer_id0=[$10], account_num=[$11], lname=[$12], fname=[$13], mi=[$14], address1=[$15], address2=[$16], address3=[$17], address4=[$18], city=[$19], state_province=[$20], postal_code=[$21], country=[$22], customer_region_id=[$23], phone1=[$24], phone2=[$25], birthdate=[$26], marital_status=[$27], yearly_income=[$28], gender=[$29], total_children=[$30], num_children_at_home=[$31], education=[$32], date_accnt_opened=[$33], member_card=[$34], occupation=[$35], houseowner=[$36], num_cars_owned=[$37], fullname=[$38], department_id=[$0], department_description=[$1])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "      EnumerableScan(model=[RELATIONAL], table=[[foodmart, department]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($2, $8)], joinType=[inner])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, sales_fact_1997]])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, customer]])\n";
        checkBushy( sql, expected );
    }


    /**
     * Tests the bushy join algorithm against a query where not all tables have a join condition to the others.
     */
    @Test
    public void testBushyCrossJoin2() throws Exception {
        final String sql = "select * from \"sales_fact_1997\" as s\n"
                + "join \"customer\" as c\n"
                + "  on s.\"customer_id\" = c.\"customer_id\"\n"
                + "cross join \"department\" as d\n"
                + "join \"employee\" as e\n"
                + "  on d.\"department_id\" = e.\"department_id\"";
        final String expected = ""
                + "EnumerableProject(model=[RELATIONAL], product_id=[$0], time_id=[$1], customer_id=[$2], promotion_id=[$3], store_id=[$4], store_sales=[$5], store_cost=[$6], unit_sales=[$7], customer_id0=[$8], account_num=[$9], lname=[$10], fname=[$11], mi=[$12], address1=[$13], address2=[$14], address3=[$15], address4=[$16], city=[$17], state_province=[$18], postal_code=[$19], country=[$20], customer_region_id=[$21], phone1=[$22], phone2=[$23], birthdate=[$24], marital_status=[$25], yearly_income=[$26], gender=[$27], total_children=[$28], num_children_at_home=[$29], education=[$30], date_accnt_opened=[$31], member_card=[$32], occupation=[$33], houseowner=[$34], num_cars_owned=[$35], fullname=[$36], department_id=[$37], department_description=[$38], employee_id=[$39], full_name=[$40], first_name=[$41], last_name=[$42], position_id=[$43], position_title=[$44], store_id0=[$45], department_id0=[$46], birth_date=[$47], hire_date=[$48], end_date=[$49], salary=[$50], supervisor_id=[$51], education_level=[$52], marital_status0=[$53], gender0=[$54], management_role=[$55])\n"
                + "  EnumerableProject(model=[RELATIONAL], product_id=[$19], time_id=[$20], customer_id=[$21], promotion_id=[$22], store_id0=[$23], store_sales=[$24], store_cost=[$25], unit_sales=[$26], customer_id0=[$27], account_num=[$28], lname=[$29], fname=[$30], mi=[$31], address1=[$32], address2=[$33], address3=[$34], address4=[$35], city=[$36], state_province=[$37], postal_code=[$38], country=[$39], customer_region_id=[$40], phone1=[$41], phone2=[$42], birthdate=[$43], marital_status0=[$44], yearly_income=[$45], gender0=[$46], total_children=[$47], num_children_at_home=[$48], education=[$49], date_accnt_opened=[$50], member_card=[$51], occupation=[$52], houseowner=[$53], num_cars_owned=[$54], fullname=[$55], department_id=[$0], department_description=[$1], employee_id=[$2], full_name=[$3], first_name=[$4], last_name=[$5], position_id=[$6], position_title=[$7], store_id=[$8], department_id0=[$9], birth_date=[$10], hire_date=[$11], end_date=[$12], salary=[$13], supervisor_id=[$14], education_level=[$15], marital_status=[$16], gender=[$17], management_role=[$18])\n"
                + "    EnumerableJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($0, $9)], joinType=[inner])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, department]])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, employee]])\n"
                + "      EnumerableJoin(model=[RELATIONAL], condition=[=($2, $8)], joinType=[inner])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, sales_fact_1997]])\n"
                + "        EnumerableScan(model=[RELATIONAL], table=[[foodmart, customer]])\n";
        checkBushy( sql, expected );
    }


    /**
     * Checks that a query returns a particular plan, using a planner with MultiJoinOptimizeBushyRule enabled.
     */
    private void checkBushy( String sql, String expected ) throws Exception {
        final SchemaPlus schema = Frameworks.createRootSchema( false ).add( "foodmart", new ReflectiveSchema( new FoodmartSchema() ), NamespaceType.RELATIONAL );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( Parser.ParserConfig.DEFAULT )
                .defaultSchema( schema )
                .traitDefs( (List<AlgTraitDef>) null )
                .programs( Programs.heuristicJoinOrder( Programs.RULE_SET, true, 2 ) )
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
                        null ) )
                .build();
        Planner planner = Frameworks.getPlanner( config );
        Node parse = planner.parse( sql );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();
        AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode transform = planner.transform( 0, traitSet, convert );
        assertThat( toString( transform ), containsString( expected ) );
    }


    /**
     * Rule to convert a {@link EnumerableProject} to an {@link JdbcRules.JdbcProject}.
     */
    private static class MockJdbcProjectRule extends ConverterRule {

        private MockJdbcProjectRule( JdbcConvention out ) {
            super( EnumerableProject.class, EnumerableConvention.INSTANCE, out, "MockJdbcProjectRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final EnumerableProject project = (EnumerableProject) alg;
            return new JdbcRules.JdbcProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( getOutConvention() ),
                    convert( project.getInput(), project.getInput().getTraitSet().replace( getOutConvention() ) ),
                    project.getProjects(),
                    project.getRowType() );
        }

    }


    /**
     * Rule to convert a {@link EnumerableScan} to an {@link MockJdbcScan}.
     */
    private class MockJdbcTableRule extends ConverterRule {

        private MockJdbcTableRule( JdbcConvention out ) {
            super( EnumerableScan.class, EnumerableConvention.INSTANCE, out, "MockJdbcTableRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final EnumerableScan scan = (EnumerableScan) alg;
            return new MockJdbcScan( scan.getCluster(), scan.getTable(), (JdbcConvention) getOutConvention() );
        }

    }


    /**
     * Relational expression representing a "mock" scan of a table in a JDBC data source.
     */
    private static class MockJdbcScan extends Scan implements JdbcAlg {

        MockJdbcScan( AlgOptCluster cluster, AlgOptTable table, JdbcConvention jdbcConvention ) {
            super( cluster, cluster.traitSetOf( jdbcConvention ), table );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new MockJdbcScan( getCluster(), table, (JdbcConvention) getConvention() );
        }


        @Override
        public void register( AlgOptPlanner planner ) {
            final JdbcConvention out = (JdbcConvention) getConvention();
            for ( AlgOptRule rule : JdbcRules.rules( out ) ) {
                planner.addRule( rule );
            }
        }


        @Override
        public JdbcImplementor.Result implement( JdbcImplementor implementor ) {
            return null;
        }

    }


    /**
     * Test to determine whether de-correlation correctly removes Correlator.
     */
    @Test
    public void testOldJoinStyleDeCorrelation() throws Exception {
        assertFalse(
                checkTpchQuery( "select\n p.`pPartkey`\n"
                        + "from\n"
                        + "  `tpch`.`part` p,\n"
                        + "  `tpch`.`partsupp` ps1\n"
                        + "where\n"
                        + "  p.`pPartkey` = ps1.`psPartkey`\n"
                        + "  and ps1.`psSupplyCost` = (\n"
                        + "    select\n"
                        + "      min(ps.`psSupplyCost`)\n"
                        + "    from\n"
                        + "      `tpch`.`partsupp` ps\n"
                        + "    where\n"
                        + "      p.`pPartkey` = ps.`psPartkey`\n"
                        + "  )\n" )
                        .contains( "Correlat" ) );
    }


    public String checkTpchQuery( String tpchTestQuery ) throws Exception {
        final SchemaPlus schema = Frameworks.createRootSchema( false ).add( "tpch", new ReflectiveSchema( new TpchSchema() ), NamespaceType.RELATIONAL );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( Parser.configBuilder().setLex( Lex.MYSQL ).build() )
                .defaultSchema( schema )
                .programs( Programs.ofRules( Programs.RULE_SET ) )
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
                        null ) )
                .build();
        String plan;
        try ( Planner p = Frameworks.getPlanner( config ) ) {
            Node n = p.parse( tpchTestQuery );
            n = p.validate( n );
            AlgNode r = p.alg( n ).project();
            plan = AlgOptUtil.toString( r );
        }
        return plan;
    }


    /**
     * User-defined aggregate function.
     */
    public static class MyCountAggFunction extends SqlAggFunction {

        public MyCountAggFunction() {
            super( "MY_COUNT", null, Kind.OTHER_FUNCTION, ReturnTypes.BIGINT, null, OperandTypes.ANY,
                    FunctionCategory.NUMERIC, false, false, Optionality.FORBIDDEN );
        }


        @Override
        public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
            // Check for COUNT(*) function.  If it is we don't want to try and derive the "*"
            if ( call.isCountStar() ) {
                return validator.getTypeFactory().createPolyType( PolyType.BIGINT );
            }
            return super.deriveType( validator, scope, call );
        }

    }


    /**
     * Test case for "ArrayIndexOutOfBoundsException when deducing collation".
     */
    @Test
    public void testOrderByNonSelectColumn() throws Exception {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "tpch", new ReflectiveSchema( new TpchSchema() ), NamespaceType.RELATIONAL );

        String query = "select t.psPartkey from \n"
                + "(select ps.psPartkey from `tpch`.`partsupp` ps \n"
                + "order by ps.psPartkey, ps.psSupplyCost) t \n"
                + "order by t.psPartkey";

        List<AlgTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        traitDefs.add( AlgCollationTraitDef.INSTANCE );
        final ParserConfig parserConfig = Parser.configBuilder().setLex( Lex.MYSQL ).build();
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .defaultSchema( schema )
                .traitDefs( traitDefs )
                .programs( Programs.ofRules( Programs.RULE_SET ) )
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
                        null ) )
                .build();
        String plan;
        try ( Planner p = Frameworks.getPlanner( config ) ) {
            Node n = p.parse( query );
            n = p.validate( n );
            AlgNode r = p.alg( n ).project();
            plan = AlgOptUtil.toString( r );
            plan = Util.toLinux( plan );
        }
        assertThat(
                plan,
                equalTo( "LogicalSort(sort0=[$0], dir0=[ASC])\n"
                        + "  LogicalProject(psPartkey=[$0])\n"
                        + "    EnumerableScan(table=[[tpch, partsupp]])\n" ) );
    }


    /**
     * Test case for "Update ProjectMergeRule description for new naming convention".
     */
    @Test
    public void testMergeProjectForceMode() throws Exception {
        RuleSet ruleSet = RuleSets.ofList( new ProjectMergeRule( true, AlgBuilder.proto( AlgFactories.DEFAULT_PROJECT_FACTORY ) ) );
        Planner planner = getPlanner( null, Programs.of( ruleSet ) );
        planner.close();
    }


    // TODO
    @Test
    @Ignore
    public void testView() throws Exception {
        final String sql = "select * FROM dept";
        final String expected = "LogicalProject(model=[RELATIONAL], DEPTNO=[$0], DNAME=[$1])\n"
                + "  LogicalValues(model=[RELATIONAL], type=[RecordType(INTEGER DEPTNO, CHAR(11) DNAME)], tuples=[[{ 10, 'Sales      ' }, { 20, 'Marketing  ' }, { 30, 'Engineering' }, { 40, 'Empty      ' }]])\n";
        checkView( sql, is( expected ) );
    }


    // TODO:
    @Test
    @Ignore
    public void testViewOnView() throws Exception {
        final String sql = "select * FROM dept30";
        final String expected = "LogicalProject(model=[RELATIONAL], DEPTNO=[$0], DNAME=[$1])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[=($0, 30)])\n"
                + "    LogicalProject(model=[RELATIONAL], DEPTNO=[$0], DNAME=[$1])\n"
                + "      LogicalValues(model=[RELATIONAL], type=[RecordType(INTEGER DEPTNO, CHAR(11) DNAME)], tuples=[[{ 10, 'Sales      ' }, { 20, 'Marketing  ' }, { 30, 'Engineering' }, { 40, 'Empty      ' }]])\n";
        checkView( sql, is( expected ) );
    }


    private void checkView( String sql, Matcher<String> matcher ) throws NodeParseException, ValidationException, AlgConversionException {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
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
                        null ) )
                .build();
        final Planner planner = Frameworks.getPlanner( config );
        Node parse = planner.parse( sql );
        final Node validate = planner.validate( parse );
        final AlgRoot root = planner.alg( validate );
        assertThat( toString( root.alg ), matcher );
    }

}
