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
import org.polypheny.db.nodes.CallBinding;


/**
 * ExplicitOperandTypeInferences implements {@link PolyOperandTypeInference} by explicitly supplying a type for each parameter.
 */
public class ExplicitOperandTypeInference implements PolyOperandTypeInference {

    private final ImmutableList<AlgDataType> paramTypes;


    /**
     * Use {@link InferTypes#explicit(java.util.List)}.
     */
    ExplicitOperandTypeInference( ImmutableList<AlgDataType> paramTypes ) {
        this.paramTypes = paramTypes;
    }


    @Override
    public void inferOperandTypes( CallBinding callBinding, AlgDataType returnType, AlgDataType[] operandTypes ) {
        if ( operandTypes.length != paramTypes.size() ) {
            // This call does not match the inference strategy.
            // It's likely that we're just about to give a validation error.
            // Don't make a fuss, just give up.
            return;
        }
        paramTypes.toArray( operandTypes );
    }

}

