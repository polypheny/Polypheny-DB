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

package org.polypheny.db.type.checker;


import java.io.Serializable;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.OperandCountRange;


/**
 * Strategy interface to check for allowed operand types of an operator call.
 * <p>
 * This interface is an example of the {@link org.polypheny.db.util.Glossary#STRATEGY_PATTERN strategy pattern}.
 */
public interface PolyOperandTypeChecker extends Serializable {

    /**
     * Checks the types of all operands to an operator call.
     *
     * @param callBinding description of the call to be checked
     * @param throwOnFailure whether to throw an exception if check fails (otherwise returns false in that case)
     * @return whether check succeeded
     */
    boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure );

    /**
     * @return range of operand counts allowed in a call
     */
    OperandCountRange getOperandCountRange();

    /**
     * Returns a string describing the allowed formal signatures of a call, e.g. "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     *
     * @param op the operator being checked
     * @param opName name to use for the operator in case of aliasing
     * @return generated string
     */
    String getAllowedSignatures( Operator op, String opName );

    /**
     * Returns the strategy for making the arguments have consistency types.
     */
    Consistency getConsistency();

    /**
     * Returns whether the {@code i}th operand is optional.
     */
    boolean isOptional( int i );

    /**
     * Strategy used to make arguments consistent.
     */
    enum Consistency {
        /**
         * Do not try to make arguments consistent.
         */
        NONE,
        /**
         * Make arguments of consistent type using comparison semantics.
         * Character values are implicitly converted to numeric, date-time, interval or boolean.
         */
        COMPARE,
        /**
         * Convert all arguments to the least restrictive type.
         */
        LEAST_RESTRICTIVE
    }

}

