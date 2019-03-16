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


import ch.unibas.dmi.dbis.polyphenydb.plan.Contexts;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SetOp;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that pushes a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter} past a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.SetOp}.
 */
public class FilterSetOpTransposeRule extends RelOptRule {

    public static final FilterSetOpTransposeRule INSTANCE = new FilterSetOpTransposeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterSetOpTransposeRule.
     */
    public FilterSetOpTransposeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( Filter.class, operand( SetOp.class, any() ) ),
                relBuilderFactory, null );
    }


    @Deprecated // to  be removed before 2.0
    public FilterSetOpTransposeRule( RelFactories.FilterFactory filterFactory ) {
        this( RelBuilder.proto( Contexts.of( filterFactory ) ) );
    }


    // implement RelOptRule
    public void onMatch( RelOptRuleCall call ) {
        Filter filterRel = call.rel( 0 );
        SetOp setOp = call.rel( 1 );

        RexNode condition = filterRel.getCondition();

        // create filters on top of each setop child, modifying the filter condition to reference each setop child
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        List<RelDataTypeField> origFields = setOp.getRowType().getFieldList();
        int[] adjustments = new int[origFields.size()];
        final List<RelNode> newSetOpInputs = new ArrayList<>();
        for ( RelNode input : setOp.getInputs() ) {
            RexNode newCondition = condition.accept( new RelOptUtil.RexInputConverter( rexBuilder, origFields, input.getRowType().getFieldList(), adjustments ) );
            newSetOpInputs.add( relBuilder.push( input ).filter( newCondition ).build() );
        }

        // create a new setop whose children are the filters created above
        SetOp newSetOp = setOp.copy( setOp.getTraitSet(), newSetOpInputs );

        call.transformTo( newSetOp );
    }
}

