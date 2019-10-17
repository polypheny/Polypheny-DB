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

package ch.unibas.dmi.dbis.polyphenydb.rel.convert;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperandChildPolicy;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * TraitMatchingRule adapts a converter rule, restricting it to fire only when its input already matches the expected output trait. This can be used with {@link HepPlanner}
 * in cases where alternate implementations are available and it is desirable to minimize converters.
 */
public class TraitMatchingRule extends RelOptRule {

    private final ConverterRule converter;


    @Deprecated // to be removed before 2.0
    public TraitMatchingRule( ConverterRule converterRule ) {
        this( converterRule, RelFactories.LOGICAL_BUILDER );
    }


    /**
     * Creates a TraitMatchingRule.
     *
     * @param converterRule Rule to be restricted; rule must take a single operand expecting a single input
     * @param relBuilderFactory Builder for relational expressions
     */
    public TraitMatchingRule( ConverterRule converterRule, RelBuilderFactory relBuilderFactory ) {
        super(
                operand( converterRule.getOperand().getMatchedClass(), operand( RelNode.class, any() ) ),
                relBuilderFactory,
                "TraitMatchingRule: " + converterRule );
        assert converterRule.getOperand().childPolicy == RelOptRuleOperandChildPolicy.ANY;
        this.converter = converterRule;
    }


    @Override
    public Convention getOutConvention() {
        return converter.getOutConvention();
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        RelNode input = call.rel( 1 );
        if ( input.getTraitSet().contains( converter.getOutTrait() ) ) {
            converter.onMatch( call );
        }
    }
}

