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
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.OperatorImpl;
import org.polypheny.db.util.Glossary;


/**
 * Strategy interface to infer the type of an operator call from the type of the operands.
 * <p>
 * This interface is an example of the {@link Glossary#STRATEGY_PATTERN strategy pattern}. This makes sense because many
 * operators have similar, straightforward strategies, such as to take the type of the first operand.
 */
public interface PolyReturnTypeInference {

    /**
     * Infers the return type of a call to an {@link OperatorImpl}.
     *
     * @param opBinding description of operator binding
     * @return inferred type; may be null
     */
    AlgDataType inferReturnType( OperatorBinding opBinding );

}

