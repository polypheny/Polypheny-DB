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

package org.polypheny.db.rel.rules;


import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.jdbc.ContextImpl;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schemas.HrClusteredSchema;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RuleSet;
import org.polypheny.db.tools.RuleSets;
import org.polypheny.db.util.Util;


/**
 * Tests the application of the {@link SortRemoveRule}.
 */
public final class SortRemoveRuleTest {

    /**
     * The default schema that is used in these tests provides tables sorted on the primary key. Due to this scan operators always come with a {@link RelCollation} trait.
     */
    private RelNode transform( String sql, RuleSet prepareRules ) throws Exception {
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        final SchemaPlus defSchema = rootSchema.add( "hr", new HrClusteredSchema(), SchemaType.RELATIONAL );
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
