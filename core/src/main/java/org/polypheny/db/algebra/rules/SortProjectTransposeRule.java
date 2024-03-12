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

package org.polypheny.db.algebra.rules;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCallBinding;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Planner rule that pushes a {@link Sort} past a {@link Project}.
 *
 * @see ProjectSortTransposeRule
 */
public class SortProjectTransposeRule extends AlgOptRule {

    public static final SortProjectTransposeRule INSTANCE = new SortProjectTransposeRule( Sort.class, LogicalRelProject.class, AlgFactories.LOGICAL_BUILDER, null );


    /**
     * Creates a SortProjectTransposeRule.
     */
    public SortProjectTransposeRule( Class<? extends Sort> sortClass, Class<? extends Project> projectClass, AlgBuilderFactory algBuilderFactory, String description ) {
        this(
                operand( sortClass, operand( projectClass, any() ) ),
                algBuilderFactory, description );
    }


    /**
     * Creates a SortProjectTransposeRule with an operand.
     */
    protected SortProjectTransposeRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        super( operand, algBuilderFactory, description );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        final Project project = call.alg( 1 );
        final AlgCluster cluster = project.getCluster();

        if ( sort.getConvention() != project.getConvention() ) {
            return;
        }

        // Determine mapping between project input and output fields. If sort relies on non-trivial expressions, we can't push.
        final TargetMapping map = AlgOptUtil.permutationIgnoreCast( project.getProjects(), project.getInput().getTupleType() );
        for ( AlgFieldCollation fc : sort.getCollation().getFieldCollations() ) {
            if ( map.getTargetOpt( fc.getFieldIndex() ) < 0 ) {
                return;
            }
            final RexNode node = project.getProjects().get( fc.getFieldIndex() );
            if ( node.isA( Kind.CAST ) ) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                final RexCall cast = (RexCall) node;
                final RexCallBinding binding = RexCallBinding.create( cluster.getTypeFactory(), cast, ImmutableList.of( AlgCollations.of( RexUtil.apply( map, fc ) ) ) );
                if ( cast.getOperator().getMonotonicity( binding ) == Monotonicity.NOT_MONOTONIC ) {
                    return;
                }
            }
        }
        final AlgCollation newCollation = cluster.traitSet().canonize( RexUtil.apply( map, sort.getCollation() ) );
        final Sort newSort =
                sort.copy(
                        sort.getTraitSet().replace( newCollation ),
                        project.getInput(),
                        newCollation,
                        null,
                        sort.offset,
                        sort.fetch );
        AlgNode newProject = project.copy( sort.getTraitSet(), ImmutableList.of( newSort ) );
        // Not only is newProject equivalent to sort; newSort is equivalent to project's input (but only if the sort is not also applying an offset/limit).
        Map<AlgNode, AlgNode> equiv;
        if ( sort.offset == null && sort.fetch == null && cluster.getPlanner().getAlgTraitDefs().contains( AlgCollationTraitDef.INSTANCE ) ) {
            equiv = ImmutableMap.of( newSort, project.getInput() );
        } else {
            equiv = ImmutableMap.of();
        }
        call.transformTo( newProject, equiv );
    }

}

