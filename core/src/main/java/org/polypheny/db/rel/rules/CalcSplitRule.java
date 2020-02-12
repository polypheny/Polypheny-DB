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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;


/**
 * Planner rule that converts a {@link Calc} to a {@link Project} and {@link Filter}.
 *
 * Not enabled by default, as it works against the usual flow, which is to convert {@code Project} and {@code Filter} to {@code Calc}. But useful for specific tasks,
 * such as optimizing before calling an {@link ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreter}.
 */
public class CalcSplitRule extends RelOptRule {

    public static final CalcSplitRule INSTANCE = new CalcSplitRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a CalcSplitRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public CalcSplitRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( Calc.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Calc calc = call.rel( 0 );
        final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = calc.getProgram().split();
        final RelBuilder relBuilder = call.builder();
        relBuilder.push( calc.getInput() );
        relBuilder.filter( projectFilter.right );
        relBuilder.project( projectFilter.left, calc.getRowType().getFieldNames() );
        call.transformTo( relBuilder.build() );
    }
}

