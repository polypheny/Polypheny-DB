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

package org.polypheny.db.algebra.rules;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link LogicalFilter} to a {@link LogicalCalc}.
 *
 * The rule does <em>NOT</em> fire if the child is a
 * {@link LogicalFilter} or a
 * {@link LogicalProject} (we assume they they
 * will be converted using {@link FilterToCalcRule} or
 * {@link ProjectToCalcRule}) or a
 * {@link LogicalCalc}. This
 * {@link LogicalFilter} will eventually be
 * converted by {@link FilterCalcMergeRule}.
 */
public class FilterToCalcRule extends AlgOptRule {

    public static final FilterToCalcRule INSTANCE = new FilterToCalcRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterToCalcRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public FilterToCalcRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalFilter.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalFilter filter = call.alg( 0 );
        final AlgNode alg = filter.getInput();

        final LogicalCalc calc = from( filter, alg );
        call.transformTo( calc );
    }


    public static LogicalCalc from( LogicalFilter filter, AlgNode alg ) {
        // Create a program containing a filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final AlgDataType inputRowType = alg.getRowType();
        final RexProgramBuilder programBuilder = new RexProgramBuilder( inputRowType, rexBuilder );
        programBuilder.addIdentity();
        programBuilder.addCondition( filter.getCondition() );
        final RexProgram program = programBuilder.getProgram();

        return LogicalCalc.create( alg, program );
    }

}

