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


import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link Filter} past a {@link Project}.
 */
public class FilterProjectTransposeRule extends AlgOptRule {

    /**
     * The default instance of {@link org.polypheny.db.algebra.rules.FilterProjectTransposeRule}.
     *
     * It matches any kind of {@link org.polypheny.db.algebra.core.Join} or {@link Filter}, and generates the same kind of Join and Filter.
     *
     * It does not allow a Filter to be pushed past the Project if {@link RexUtil#containsCorrelation there is a correlation condition}) anywhere in the Filter, since in some cases it can
     * prevent a {@link Correlate} from being de-correlated.
     */
    public static final FilterProjectTransposeRule INSTANCE = new FilterProjectTransposeRule( Filter.class, Project.class, true, true, AlgFactories.LOGICAL_BUILDER );

    private final boolean copyFilter;
    private final boolean copyProject;


    /**
     * Creates a FilterProjectTransposeRule.
     *
     * Equivalent to the rule created by {@link #FilterProjectTransposeRule(Class, Predicate, Class, Predicate, boolean, boolean, AlgBuilderFactory)} with some default predicates that do not allow a filter to be pushed
     * past the project if there is a correlation condition anywhere in the filter (since in some cases it can prevent a {@link Correlate} from being de-correlated).
     */
    public FilterProjectTransposeRule( Class<? extends Filter> filterClass, Class<? extends Project> projectClass, boolean copyFilter, boolean copyProject, AlgBuilderFactory algBuilderFactory ) {
        this( filterClass,
                filter -> !RexUtil.containsCorrelation( filter.getCondition() ),
                projectClass, project -> true,
                copyFilter,
                copyProject,
                algBuilderFactory );
    }


    /**
     * Creates a FilterProjectTransposeRule.
     *
     * If {@code copyFilter} is true, creates the same kind of Filter as matched in the rule, otherwise it creates a Filter using the{@link AlgBuilder}  obtained by the {@code algBuilderFactory}.
     * Similarly for {@code copyProject}.
     *
     * Defining predicates for the Filter (using {@code filterPredicate}) and/or the Project (using {@code projectPredicate} allows making the rule more restrictive.
     */
    public <F extends Filter, P extends Project> FilterProjectTransposeRule( Class<F> filterClass, Predicate<? super F> filterPredicate, Class<P> projectClass, Predicate<? super P> projectPredicate, boolean copyFilter, boolean copyProject, AlgBuilderFactory algBuilderFactory ) {
        this(
                operand( filterClass, null, filterPredicate, operand( projectClass, null, projectPredicate, any() ) ),
                copyFilter, copyProject, algBuilderFactory );
    }


    protected FilterProjectTransposeRule( AlgOptRuleOperand operand, boolean copyFilter, boolean copyProject, AlgBuilderFactory algBuilderFactory ) {
        super( operand, algBuilderFactory, null );
        this.copyFilter = copyFilter;
        this.copyProject = copyProject;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Filter filter = call.alg( 0 );
        final Project project = call.alg( 1 );

        if ( RexOver.containsOver( project.getProjects(), null ) ) {
            // In general a filter cannot be pushed below a windowing calculation.
            // Applying the filter before the aggregation function changes the results of the windowing invocation.
            //
            // When the filter is on the PARTITION BY expression of the OVER clause it can be pushed down. For now we don't support this.
            return;
        }
        // convert the filter to one that references the child of the project
        RexNode newCondition = AlgOptUtil.pushPastProject( filter.getCondition(), project );

        final AlgBuilder algBuilder = call.builder();
        AlgNode newFilterRel;
        if ( copyFilter ) {
            newFilterRel = filter.copy( filter.getTraitSet(), project.getInput(), RexUtil.removeNullabilityCast( algBuilder.getTypeFactory(), newCondition ) );
        } else {
            newFilterRel = algBuilder.push( project.getInput() ).filter( newCondition ).build();
        }

        AlgNode newProjRel =
                copyProject
                        ? project.copy( project.getTraitSet(), newFilterRel, project.getProjects(), project.getTupleType() )
                        : algBuilder.push( newFilterRel )
                                .project( project.getProjects(), project.getTupleType().getFieldNames() )
                                .build();

        call.transformTo( newProjRel );
    }

}
