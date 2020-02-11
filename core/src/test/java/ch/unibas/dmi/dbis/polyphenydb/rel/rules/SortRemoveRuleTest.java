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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRules;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schemas.HrClusteredSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.SqlParserConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.tools.RuleSet;
import ch.unibas.dmi.dbis.polyphenydb.tools.RuleSets;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.Arrays;
import org.junit.Test;


/**
 * Tests the application of the {@link SortRemoveRule}.
 */
public final class SortRemoveRuleTest {

    /**
     * The default schema that is used in these tests provides tables sorted on the primary key. Due to this scan operators always come with a {@link RelCollation} trait.
     */
    private RelNode transform( String sql, RuleSet prepareRules ) throws Exception {
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        final SchemaPlus defSchema = rootSchema.add( "hr", new HrClusteredSchema() );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( SqlParserConfig.DEFAULT )
                .defaultSchema( defSchema )
                .traitDefs( ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE )
                .programs( Programs.of( prepareRules ), Programs.ofRules( SortRemoveRule.INSTANCE ) )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( rootSchema ),
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
        SqlNode parse = planner.parse( sql );
        SqlNode validate = planner.validate( parse );
        RelRoot planRoot = planner.rel( validate );
        RelNode planBefore = planRoot.rel;
        RelTraitSet desiredTraits = planBefore.getTraitSet()
                .replace( EnumerableConvention.INSTANCE )
                .replace( planRoot.collation ).simplify();
        RelNode planAfter = planner.transform( 0, desiredTraits, planBefore );
        return planner.transform( 1, desiredTraits, planAfter );
    }


    /**
     * Test case for "Enrich enumerable join operators with order preserving information".
     *
     * Since join inputs are sorted, and this join preserves the order of the left input, there shouldn't be any sort operator above the join.
     */
    @Test
    public void removeSortOverEnumerableJoin() throws Exception {
        RuleSet prepareRules =
                RuleSets.ofList(
                        SortProjectTransposeRule.INSTANCE,
                        EnumerableRules.ENUMERABLE_JOIN_RULE,
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_SORT_RULE,
                        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE );
        for ( String joinType : Arrays.asList( "left", "right", "full", "inner" ) ) {
            String sql = "select e.\"deptno\" from \"hr\".\"emps\" e " + joinType + " join \"hr\".\"depts\" d on e.\"deptno\" = d.\"deptno\" order by e.\"empid\" ";
            RelNode actualPlan = transform( sql, prepareRules );
            assertThat(
                    toString( actualPlan ),
                    allOf( containsString( "EnumerableJoin" ), not( containsString( "EnumerableSort" ) ) ) );
        }
    }


    /**
     * Test case for "Enrich enumerable join operators with order preserving information".
     *
     * Since join inputs are sorted, and this join preserves the order of the left input, there shouldn't be any sort operator above the join.
     */
    @Test
    public void removeSortOverEnumerableThetaJoin() throws Exception {
        RuleSet prepareRules =
                RuleSets.ofList(
                        SortProjectTransposeRule.INSTANCE,
                        EnumerableRules.ENUMERABLE_JOIN_RULE,
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_SORT_RULE,
                        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE );
        // Inner join is not considered since the ENUMERABLE_JOIN_RULE does not generate a theta join in the case of inner joins.
        for ( String joinType : Arrays.asList( "left", "right", "full" ) ) {
            String sql = "select e.\"deptno\" from \"hr\".\"emps\" e " + joinType + " join \"hr\".\"depts\" d on e.\"deptno\" > d.\"deptno\" order by e.\"empid\" ";
            RelNode actualPlan = transform( sql, prepareRules );
            assertThat(
                    toString( actualPlan ),
                    allOf( containsString( "EnumerableThetaJoin" ), not( containsString( "EnumerableSort" ) ) ) );
        }
    }


    /**
     * Test case for "Enrich enumerable join operators with order preserving information".
     *
     * Since join inputs are sorted, and this join preserves the order of the left input, there shouldn't be any sort operator above the join.
     */
    @Test
    public void removeSortOverEnumerableCorrelate() throws Exception {
        RuleSet prepareRules =
                RuleSets.ofList(
                        SortProjectTransposeRule.INSTANCE,
                        JoinToCorrelateRule.INSTANCE,
                        EnumerableRules.ENUMERABLE_SORT_RULE,
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                        EnumerableRules.ENUMERABLE_FILTER_RULE,
                        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE );
        for ( String joinType : Arrays.asList( "left", "inner" ) ) {
            String sql = "select e.\"deptno\" from \"hr\".\"emps\" e " + joinType + " join \"hr\".\"depts\" d on e.\"deptno\" = d.\"deptno\" order by e.\"empid\" ";
            RelNode actualPlan = transform( sql, prepareRules );
            assertThat(
                    toString( actualPlan ),
                    allOf( containsString( "EnumerableCorrelate" ), not( containsString( "EnumerableSort" ) ) ) );
        }
    }


    /**
     * Test case for "Enrich enumerable join operators with order preserving information".
     *
     * Since join inputs are sorted, and this join preserves the order of the left input, there shouldn't be any sort operator above the join.
     */
    @Test
    public void removeSortOverEnumerableSemiJoin() throws Exception {
        RuleSet prepareRules =
                RuleSets.ofList(
                        SortProjectTransposeRule.INSTANCE,
                        SemiJoinRule.PROJECT,
                        SemiJoinRule.JOIN,
                        EnumerableRules.ENUMERABLE_PROJECT_RULE,
                        EnumerableRules.ENUMERABLE_SORT_RULE,
                        EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                        EnumerableRules.ENUMERABLE_FILTER_RULE,
                        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE );
        String sql = "select e.\"deptno\" from \"hr\".\"emps\" e\n"
                + " where e.\"deptno\" in (select d.\"deptno\" from \"hr\".\"depts\" d)\n"
                + " order by e.\"empid\"";
        RelNode actualPlan = transform( sql, prepareRules );
        assertThat(
                toString( actualPlan ),
                allOf( containsString( "EnumerableSemiJoin" ), not( containsString( "EnumerableSort" ) ) ) );
    }


    private String toString( RelNode rel ) {
        return Util.toLinux( RelOptUtil.dumpPlan( "", rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
    }

}
