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
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that removes a trivial {@link org.polypheny.db.rel.logical.LogicalCalc}.
 *
 * A {@link org.polypheny.db.rel.logical.LogicalCalc} is trivial if it projects its input fields in their original order, and it does not filter.
 *
 * @see ProjectRemoveRule
 */
public class CalcRemoveRule extends RelOptRule {

    public static final CalcRemoveRule INSTANCE = new CalcRemoveRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a CalcRemoveRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public CalcRemoveRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( LogicalCalc.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        LogicalCalc calc = call.rel( 0 );
        RexProgram program = calc.getProgram();
        if ( !program.isTrivial() ) {
            return;
        }
        RelNode input = calc.getInput();
        input = call.getPlanner().register( input, calc );
        call.transformTo( convert( input, calc.getTraitSet() ) );
    }
}

