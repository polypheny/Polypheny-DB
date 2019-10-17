/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;


/**
 * Planner rule that pushes a {@link Sort} past a {@link Project}.
 *
 * @see ProjectSortTransposeRule
 */
public class SortProjectTransposeRule extends RelOptRule {

    public static final SortProjectTransposeRule INSTANCE = new SortProjectTransposeRule( Sort.class, LogicalProject.class, RelFactories.LOGICAL_BUILDER, null );


    @Deprecated // to be removed before 2.0
    public SortProjectTransposeRule( Class<? extends Sort> sortClass, Class<? extends Project> projectClass ) {
        this( sortClass, projectClass, RelFactories.LOGICAL_BUILDER, null );
    }


    @Deprecated // to be removed before 2.0
    public SortProjectTransposeRule( Class<? extends Sort> sortClass, Class<? extends Project> projectClass, String description ) {
        this( sortClass, projectClass, RelFactories.LOGICAL_BUILDER, description );
    }


    /**
     * Creates a SortProjectTransposeRule.
     */
    public SortProjectTransposeRule( Class<? extends Sort> sortClass, Class<? extends Project> projectClass, RelBuilderFactory relBuilderFactory, String description ) {
        this(
                operand( sortClass, operand( projectClass, any() ) ),
                relBuilderFactory, description );
    }


    /**
     * Creates a SortProjectTransposeRule with an operand.
     */
    protected SortProjectTransposeRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    @Deprecated // to be removed before 2.0
    protected SortProjectTransposeRule( RelOptRuleOperand operand ) {
        super( operand );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Sort sort = call.rel( 0 );
        final Project project = call.rel( 1 );
        final RelOptCluster cluster = project.getCluster();

        if ( sort.getConvention() != project.getConvention() ) {
            return;
        }

        // Determine mapping between project input and output fields. If sort relies on non-trivial expressions, we can't push.
        final TargetMapping map = RelOptUtil.permutationIgnoreCast( project.getProjects(), project.getInput().getRowType() );
        for ( RelFieldCollation fc : sort.getCollation().getFieldCollations() ) {
            if ( map.getTargetOpt( fc.getFieldIndex() ) < 0 ) {
                return;
            }
            final RexNode node = project.getProjects().get( fc.getFieldIndex() );
            if ( node.isA( SqlKind.CAST ) ) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                final RexCall cast = (RexCall) node;
                final RexCallBinding binding = RexCallBinding.create( cluster.getTypeFactory(), cast, ImmutableList.of( RelCollations.of( RexUtil.apply( map, fc ) ) ) );
                if ( cast.getOperator().getMonotonicity( binding ) == SqlMonotonicity.NOT_MONOTONIC ) {
                    return;
                }
            }
        }
        final RelCollation newCollation = cluster.traitSet().canonize( RexUtil.apply( map, sort.getCollation() ) );
        final Sort newSort =
                sort.copy(
                        sort.getTraitSet().replace( newCollation ),
                        project.getInput(),
                        newCollation,
                        sort.offset,
                        sort.fetch );
        RelNode newProject = project.copy( sort.getTraitSet(), ImmutableList.of( newSort ) );
        // Not only is newProject equivalent to sort; newSort is equivalent to project's input (but only if the sort is not also applying an offset/limit).
        Map<RelNode, RelNode> equiv;
        if ( sort.offset == null && sort.fetch == null && cluster.getPlanner().getRelTraitDefs().contains( RelCollationTraitDef.INSTANCE ) ) {
            equiv = ImmutableMap.of( (RelNode) newSort, project.getInput() );
        } else {
            equiv = ImmutableMap.of();
        }
        call.transformTo( newProject, equiv );
    }
}

