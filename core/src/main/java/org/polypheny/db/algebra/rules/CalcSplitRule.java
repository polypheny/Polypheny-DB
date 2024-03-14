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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Pair;


/**
 * Planner rule that converts a {@link Calc} to a {@link Project} and {@link Filter}.
 *
 * Not enabled by default, as it works against the usual flow, which is to convert {@code Project} and {@code Filter} to {@code Calc}. But useful for specific tasks,
 * such as optimizing before calling an {@link org.polypheny.db.interpreter.Interpreter}.
 */
public class CalcSplitRule extends AlgOptRule {

    public static final CalcSplitRule INSTANCE = new CalcSplitRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a CalcSplitRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public CalcSplitRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( Calc.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Calc calc = call.alg( 0 );
        final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = calc.getProgram().split();
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( calc.getInput() );
        algBuilder.filter( projectFilter.right );
        algBuilder.project( projectFilter.left, calc.getTupleType().getFieldNames() );
        call.transformTo( algBuilder.build() );
    }

}

