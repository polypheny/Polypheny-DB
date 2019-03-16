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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;


/**
 * Strategy interface to check for allowed operand types of an operator call.
 *
 * This interface is an example of the {@link ch.unibas.dmi.dbis.polyphenydb.util.Glossary#STRATEGY_PATTERN strategy pattern}.
 */
public interface SqlOperandTypeChecker {

    /**
     * Checks the types of all operands to an operator call.
     *
     * @param callBinding description of the call to be checked
     * @param throwOnFailure whether to throw an exception if check fails (otherwise returns false in that case)
     * @return whether check succeeded
     */
    boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure );

    /**
     * @return range of operand counts allowed in a call
     */
    SqlOperandCountRange getOperandCountRange();

    /**
     * Returns a string describing the allowed formal signatures of a call, e.g. "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     *
     * @param op the operator being checked
     * @param opName name to use for the operator in case of aliasing
     * @return generated string
     */
    String getAllowedSignatures( SqlOperator op, String opName );

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

