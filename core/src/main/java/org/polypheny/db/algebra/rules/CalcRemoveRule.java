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
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that removes a trivial {@link LogicalCalc}.
 *
 * A {@link LogicalCalc} is trivial if it projects its input fields in their original order, and it does not filter.
 *
 * @see ProjectRemoveRule
 */
public class CalcRemoveRule extends AlgOptRule {

    public static final CalcRemoveRule INSTANCE = new CalcRemoveRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a CalcRemoveRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public CalcRemoveRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalCalc.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalCalc calc = call.alg( 0 );
        RexProgram program = calc.getProgram();
        if ( !program.isTrivial() ) {
            return;
        }
        AlgNode input = calc.getInput();
        input = call.getPlanner().register( input, calc );
        call.transformTo( convert( input, calc.getTraitSet() ) );
    }

}

