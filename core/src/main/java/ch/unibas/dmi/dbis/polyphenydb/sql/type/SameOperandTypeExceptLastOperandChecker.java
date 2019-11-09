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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;


/**
 * Parameter type-checking strategy where all operand types except last one must be the same.
 */
public class SameOperandTypeExceptLastOperandChecker extends SameOperandTypeChecker {

    protected final String lastOperandTypeName;


    public SameOperandTypeExceptLastOperandChecker( int nOperands, String lastOperandTypeName ) {
        super( nOperands );
        this.lastOperandTypeName = lastOperandTypeName;
    }


    @Override
    protected boolean checkOperandTypesImpl( SqlOperatorBinding operatorBinding, boolean throwOnFailure, SqlCallBinding callBinding ) {
        int nOperandsActual = nOperands;
        if ( nOperandsActual == -1 ) {
            nOperandsActual = operatorBinding.getOperandCount();
        }
        assert !(throwOnFailure && (callBinding == null));
        RelDataType[] types = new RelDataType[nOperandsActual];
        final List<Integer> operandList = getOperandList( operatorBinding.getOperandCount() );
        for ( int i : operandList ) {
            if ( operatorBinding.isOperandNull( i, false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( i ), Static.RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }
            types[i] = operatorBinding.getOperandType( i );
        }
        int prev = -1;
        for ( int i : operandList ) {
            if ( prev >= 0 && i != operandList.get( operandList.size() - 1 ) ) {
                if ( !SqlTypeUtil.isComparable( types[i], types[prev] ) ) {
                    if ( !throwOnFailure ) {
                        return false;
                    }

                    // REVIEW jvs: Why don't we use newValidationSignatureError() here?  It gives more specific diagnostics.
                    throw callBinding.newValidationError( Static.RESOURCE.needSameTypeParameter() );
                }
            }
            prev = i;
        }
        return true;
    }


    @Override
    public String getAllowedSignatures( SqlOperator op, String opName ) {
        final String typeName = getTypeName();
        if ( nOperands == -1 ) {
            return SqlUtil.getAliasedSignature( op, opName, ImmutableList.of( typeName, typeName, "..." ) );
        } else {
            List<String> types = Collections.nCopies( nOperands - 1, typeName );
            types.add( lastOperandTypeName );
            return SqlUtil.getAliasedSignature( op, opName, types );
        }
    }
}

