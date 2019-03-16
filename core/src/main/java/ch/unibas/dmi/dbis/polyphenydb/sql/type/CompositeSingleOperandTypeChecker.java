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
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;


/**
 * Allows multiple {@link ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlSingleOperandTypeChecker} rules to be combined into one rule.
 */
public class CompositeSingleOperandTypeChecker extends CompositeOperandTypeChecker implements SqlSingleOperandTypeChecker {

    /**
     * Package private. Use {@link OperandTypes#and}, {@link OperandTypes#or}.
     */
    CompositeSingleOperandTypeChecker( CompositeOperandTypeChecker.Composition composition, ImmutableList<? extends SqlSingleOperandTypeChecker> allowedRules, String allowedSignatures ) {
        super( composition, allowedRules, allowedSignatures, null );
    }


    @SuppressWarnings("unchecked")
    @Override
    public ImmutableList<? extends SqlSingleOperandTypeChecker> getRules() {
        return (ImmutableList<? extends SqlSingleOperandTypeChecker>) allowedRules;
    }


    public boolean checkSingleOperandType( SqlCallBinding callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure ) {
        assert allowedRules.size() >= 1;

        final ImmutableList<? extends SqlSingleOperandTypeChecker> rules = getRules();
        if ( composition == Composition.SEQUENCE ) {
            return rules.get( iFormalOperand ).checkSingleOperandType( callBinding, node, 0, throwOnFailure );
        }

        int typeErrorCount = 0;

        boolean throwOnAndFailure = (composition == Composition.AND) && throwOnFailure;

        for ( SqlSingleOperandTypeChecker rule : rules ) {
            if ( !rule.checkSingleOperandType( callBinding, node, iFormalOperand, throwOnAndFailure ) ) {
                typeErrorCount++;
            }
        }

        boolean ret;
        switch ( composition ) {
            case AND:
                ret = typeErrorCount == 0;
                break;
            case OR:
                ret = typeErrorCount < allowedRules.size();
                break;
            default:
                // should never come here
                throw Util.unexpected( composition );
        }

        if ( !ret && throwOnFailure ) {
            // In the case of a composite OR, we want to throw an error describing in more detail what the problem was, hence doing the loop again.
            for ( SqlSingleOperandTypeChecker rule : rules ) {
                rule.checkSingleOperandType( callBinding, node, iFormalOperand, true );
            }

            // If no exception thrown, just throw a generic validation signature error.
            throw callBinding.newValidationSignatureError();
        }

        return ret;
    }
}

