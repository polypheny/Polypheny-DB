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


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.CallBinding;


/**
 * Strategy to infer unknown types of the operands of an operator call.
 */
public interface PolyOperandTypeInference {

    /**
     * Infers any unknown operand types.
     *
     * @param callBinding description of the call being analyzed
     * @param returnType the type known or inferred for the result of the call
     * @param operandTypes receives the inferred types for all operands
     */
    void inferOperandTypes( CallBinding callBinding, AlgDataType returnType, AlgDataType[] operandTypes );

}

