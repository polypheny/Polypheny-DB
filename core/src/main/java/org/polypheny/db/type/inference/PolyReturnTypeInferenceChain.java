/*
 * Copyright 2019-2021 The Polypheny Project
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
 */

package org.polypheny.db.type.inference;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.OperatorBinding;


/**
 * Strategy to infer the type of an operator call from the type of the operands by using a series of
 * {@link PolyReturnTypeInference} rules in a given order. If a rule fails to find a return type (by returning NULL), next
 * rule is tried until there are no more rules in which case NULL will be returned.
 */
public class PolyReturnTypeInferenceChain implements PolyReturnTypeInference {

    private final ImmutableList<PolyReturnTypeInference> rules;


    /**
     * Creates a SqlReturnTypeInferenceChain from an array of rules.
     * <p>
     * Package-protected.
     * Use {@link ReturnTypes#chain}.</p>
     */
    PolyReturnTypeInferenceChain( PolyReturnTypeInference... rules ) {
        assert rules != null;
        assert rules.length > 1;
        for ( PolyReturnTypeInference rule : rules ) {
            assert rule != null;
        }
        this.rules = ImmutableList.copyOf( rules );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        for ( PolyReturnTypeInference rule : rules ) {
            AlgDataType ret = rule.inferReturnType( opBinding );
            if ( ret != null ) {
                return ret;
            }
        }
        return null;
    }

}

