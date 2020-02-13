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


import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalCalc;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that converts a {@link org.polypheny.db.rel.logical.LogicalFilter} to a {@link LogicalCalc}.
 *
 * The rule does <em>NOT</em> fire if the child is a
 * {@link org.polypheny.db.rel.logical.LogicalFilter} or a
 * {@link org.polypheny.db.rel.logical.LogicalProject} (we assume they they
 * will be converted using {@link FilterToCalcRule} or
 * {@link ProjectToCalcRule}) or a
 * {@link LogicalCalc}. This
 * {@link org.polypheny.db.rel.logical.LogicalFilter} will eventually be
 * converted by {@link FilterCalcMergeRule}.
 */
public class FilterToCalcRule extends RelOptRule {

    public static final FilterToCalcRule INSTANCE = new FilterToCalcRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterToCalcRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public FilterToCalcRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( LogicalFilter.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final LogicalFilter filter = call.rel( 0 );
        final RelNode rel = filter.getInput();

        // Create a program containing a filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RelDataType inputRowType = rel.getRowType();
        final RexProgramBuilder programBuilder = new RexProgramBuilder( inputRowType, rexBuilder );
        programBuilder.addIdentity();
        programBuilder.addCondition( filter.getCondition() );
        final RexProgram program = programBuilder.getProgram();

        final LogicalCalc calc = LogicalCalc.create( rel, program );
        call.transformTo( calc );
    }
}

