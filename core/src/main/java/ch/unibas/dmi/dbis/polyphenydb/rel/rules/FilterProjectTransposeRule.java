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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Correlate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexOver;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.function.Predicate;


/**
 * Planner rule that pushes a {@link Filter} past a {@link Project}.
 */
public class FilterProjectTransposeRule extends RelOptRule {

    /**
     * The default instance of {@link ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterProjectTransposeRule}.
     *
     * It matches any kind of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Join} or {@link Filter}, and generates the same kind of Join and Filter.
     *
     * It does not allow a Filter to be pushed past the Project if {@link RexUtil#containsCorrelation there is a correlation condition}) anywhere in the Filter, since in some cases it can
     * prevent a {@link Correlate} from being de-correlated.
     */
    public static final FilterProjectTransposeRule INSTANCE = new FilterProjectTransposeRule( Filter.class, Project.class, true, true, RelFactories.LOGICAL_BUILDER );

    private final boolean copyFilter;
    private final boolean copyProject;


    /**
     * Creates a FilterProjectTransposeRule.
     *
     * Equivalent to the rule created by {@link #FilterProjectTransposeRule(Class, Predicate, Class, Predicate, boolean, boolean, RelBuilderFactory)} with some default predicates that do not allow a filter to be pushed
     * past the project if there is a correlation condition anywhere in the filter (since in some cases it can prevent a {@link Correlate} from being de-correlated).
     */
    public FilterProjectTransposeRule( Class<? extends Filter> filterClass, Class<? extends Project> projectClass, boolean copyFilter, boolean copyProject, RelBuilderFactory relBuilderFactory ) {
        this( filterClass,
                filter -> !RexUtil.containsCorrelation( filter.getCondition() ),
                projectClass, project -> true,
                copyFilter,
                copyProject,
                relBuilderFactory );
    }


    /**
     * Creates a FilterProjectTransposeRule.
     *
     * If {@code copyFilter} is true, creates the same kind of Filter as matched in the rule, otherwise it creates a Filter using the RelBuilder obtained by the {@code relBuilderFactory}.
     * Similarly for {@code copyProject}.
     *
     * Defining predicates for the Filter (using {@code filterPredicate}) and/or the Project (using {@code projectPredicate} allows making the rule more restrictive.
     */
    public <F extends Filter, P extends Project> FilterProjectTransposeRule( Class<F> filterClass, Predicate<? super F> filterPredicate, Class<P> projectClass, Predicate<? super P> projectPredicate, boolean copyFilter, boolean copyProject, RelBuilderFactory relBuilderFactory ) {
        this(
                operandJ( filterClass, null, filterPredicate, operandJ( projectClass, null, projectPredicate, any() ) ),
                copyFilter, copyProject, relBuilderFactory );
    }


    @Deprecated // to be removed before 2.0
    public FilterProjectTransposeRule( Class<? extends Filter> filterClass, RelFactories.FilterFactory filterFactory, Class<? extends Project> projectClass, RelFactories.ProjectFactory projectFactory ) {
        this( filterClass,
                filter -> !RexUtil.containsCorrelation( filter.getCondition() ),
                projectClass, project -> true,
                filterFactory == null,
                projectFactory == null,
                RelBuilder.proto( filterFactory, projectFactory ) );
    }


    protected FilterProjectTransposeRule( RelOptRuleOperand operand, boolean copyFilter, boolean copyProject, RelBuilderFactory relBuilderFactory ) {
        super( operand, relBuilderFactory, null );
        this.copyFilter = copyFilter;
        this.copyProject = copyProject;
    }


    public void onMatch( RelOptRuleCall call ) {
        final Filter filter = call.rel( 0 );
        final Project project = call.rel( 1 );

        if ( RexOver.containsOver( project.getProjects(), null ) ) {
            // In general a filter cannot be pushed below a windowing calculation.
            // Applying the filter before the aggregation function changes the results of the windowing invocation.
            //
            // When the filter is on the PARTITION BY expression of the OVER clause it can be pushed down. For now we don't support this.
            return;
        }
        // convert the filter to one that references the child of the project
        RexNode newCondition = RelOptUtil.pushPastProject( filter.getCondition(), project );

        final RelBuilder relBuilder = call.builder();
        RelNode newFilterRel;
        if ( copyFilter ) {
            newFilterRel = filter.copy( filter.getTraitSet(), project.getInput(), RexUtil.removeNullabilityCast( relBuilder.getTypeFactory(), newCondition ) );
        } else {
            newFilterRel = relBuilder.push( project.getInput() ).filter( newCondition ).build();
        }

        RelNode newProjRel =
                copyProject
                        ? project.copy( project.getTraitSet(), newFilterRel, project.getProjects(), project.getRowType() )
                        : relBuilder.push( newFilterRel )
                                .project( project.getProjects(), project.getRowType().getFieldNames() )
                                .build();

        call.transformTo( newProjRel );
    }
}
