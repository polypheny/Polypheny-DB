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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * CoerceInputsRule pre-casts inputs to a particular type. This can be used to assist operator implementations which impose requirements on their input types.
 */
public class CoerceInputsRule extends AlgOptRule {

    private final Class consumerRelClass;

    private final boolean coerceNames;


    /**
     * Creates a CoerceInputsRule.
     *
     * @param consumerRelClass Class of {@link AlgNode} that will consume the inputs
     * @param coerceNames If true, coerce names and types; if false, coerce type only
     * @param algBuilderFactory Builder for relational expressions
     */
    public CoerceInputsRule( Class<? extends AlgNode> consumerRelClass, boolean coerceNames, AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( consumerRelClass, any() ),
                algBuilderFactory,
                "CoerceInputsRule:" + consumerRelClass.getName() );
        this.consumerRelClass = consumerRelClass;
        this.coerceNames = coerceNames;
    }


    @Override
    public Convention getOutConvention() {
        return Convention.NONE;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        AlgNode consumerRel = call.alg( 0 );
        if ( consumerRel.getClass() != consumerRelClass ) {
            // require exact match on type
            return;
        }
        List<AlgNode> inputs = consumerRel.getInputs();
        List<AlgNode> newInputs = new ArrayList<>( inputs );
        boolean coerce = false;
        for ( int i = 0; i < inputs.size(); ++i ) {
            AlgDataType expectedType = consumerRel.getExpectedInputRowType( i );
            AlgNode input = inputs.get( i );
            AlgNode newInput = AlgOptUtil.createCastAlg( input, expectedType, coerceNames );
            if ( newInput != input ) {
                newInputs.set( i, newInput );
                coerce = true;
            }
            assert AlgOptUtil.areRowTypesEqual( newInputs.get( i ).getTupleType(), expectedType, coerceNames );
        }
        if ( !coerce ) {
            return;
        }
        AlgNode newConsumerRel = consumerRel.copy( consumerRel.getTraitSet(), newInputs );
        call.transformTo( newConsumerRel );
    }

}
