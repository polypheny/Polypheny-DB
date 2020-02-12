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

package org.polypheny.db.rel.convert;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperandChildPolicy;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * TraitMatchingRule adapts a converter rule, restricting it to fire only when its input already matches the expected output trait. This can be used with {@link HepPlanner}
 * in cases where alternate implementations are available and it is desirable to minimize converters.
 */
public class TraitMatchingRule extends RelOptRule {

    private final ConverterRule converter;

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

