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


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.ArrayList;
import java.util.List;


/**
 * CoerceInputsRule pre-casts inputs to a particular type. This can be used to assist operator implementations which impose requirements on their input types.
 */
public class CoerceInputsRule extends RelOptRule {

    private final Class consumerRelClass;

    private final boolean coerceNames;


    /**
     * Creates a CoerceInputsRule.
     *
     * @param consumerRelClass Class of RelNode that will consume the inputs
     * @param coerceNames If true, coerce names and types; if false, coerce type only
     * @param relBuilderFactory Builder for relational expressions
     */
    public CoerceInputsRule( Class<? extends RelNode> consumerRelClass, boolean coerceNames, RelBuilderFactory relBuilderFactory ) {
        super(
                operand( consumerRelClass, any() ),
                relBuilderFactory,
                "CoerceInputsRule:" + consumerRelClass.getName() );
        this.consumerRelClass = consumerRelClass;
        this.coerceNames = coerceNames;
    }


    @Override
    public Convention getOutConvention() {
        return Convention.NONE;
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        RelNode consumerRel = call.rel( 0 );
        if ( consumerRel.getClass() != consumerRelClass ) {
            // require exact match on type
            return;
        }
        List<RelNode> inputs = consumerRel.getInputs();
        List<RelNode> newInputs = new ArrayList<>( inputs );
        boolean coerce = false;
        for ( int i = 0; i < inputs.size(); ++i ) {
            RelDataType expectedType = consumerRel.getExpectedInputRowType( i );
            RelNode input = inputs.get( i );
            RelNode newInput = RelOptUtil.createCastRel( input, expectedType, coerceNames );
            if ( newInput != input ) {
                newInputs.set( i, newInput );
                coerce = true;
            }
            assert RelOptUtil.areRowTypesEqual( newInputs.get( i ).getRowType(), expectedType, coerceNames );
        }
        if ( !coerce ) {
            return;
        }
        RelNode newConsumerRel = consumerRel.copy( consumerRel.getTraitSet(), newInputs );
        call.transformTo( newConsumerRel );
    }
}
