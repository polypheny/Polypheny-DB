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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Monotonicity;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCallBinding;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Planner rule that pushes a {@link Sort} past a {@link Project}.
 *
 * @see ProjectSortTransposeRule
 */
public class SortProjectTransposeRule extends RelOptRule {

    public static final SortProjectTransposeRule INSTANCE = new SortProjectTransposeRule( Sort.class, LogicalProject.class, RelFactories.LOGICAL_BUILDER, null );


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
            if ( node.isA( Kind.CAST ) ) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                final RexCall cast = (RexCall) node;
                final RexCallBinding binding = RexCallBinding.create( cluster.getTypeFactory(), cast, ImmutableList.of( RelCollations.of( RexUtil.apply( map, fc ) ) ) );
                if ( cast.getOperator().getMonotonicity( binding ) == Monotonicity.NOT_MONOTONIC ) {
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

