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
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;


/**
 * AssignableOperandTypeChecker implements {@link SqlOperandTypeChecker} by verifying that the type of each argument is assignable to a predefined set of parameter types (under the SQL definition of "assignable").
 */
public class AssignableOperandTypeChecker implements SqlOperandTypeChecker {

    private final List<RelDataType> paramTypes;
    private final ImmutableList<String> paramNames;


    /**
     * Instantiates this strategy with a specific set of parameter types.
     *
     * @param paramTypes parameter types for operands; index in this array corresponds to operand number
     * @param paramNames parameter names, or null
     */
    public AssignableOperandTypeChecker( List<RelDataType> paramTypes, List<String> paramNames ) {
        this.paramTypes = ImmutableList.copyOf( paramTypes );
        this.paramNames = paramNames == null ? null : ImmutableList.copyOf( paramNames );
    }


    public boolean isOptional( int i ) {
        return false;
    }


    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of( paramTypes.size() );
    }


    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        // Do not use callBinding.operands(). We have not resolved to a function yet, therefore we do not know the ordered parameter names.
        final List<SqlNode> operands = callBinding.getCall().getOperandList();
        for ( Pair<RelDataType, SqlNode> pair : Pair.zip( paramTypes, operands ) ) {
            RelDataType argType = callBinding.getValidator().deriveType( callBinding.getScope(), pair.right );
            if ( !SqlTypeUtil.canAssignFrom( pair.left, argType ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }
        return true;
    }


    public String getAllowedSignatures( SqlOperator op, String opName ) {
        StringBuilder sb = new StringBuilder();
        sb.append( opName );
        sb.append( "(" );
        for ( Ord<RelDataType> paramType : Ord.zip( paramTypes ) ) {
            if ( paramType.i > 0 ) {
                sb.append( ", " );
            }
            if ( paramNames != null ) {
                sb.append( paramNames.get( paramType.i ) ).append( " => " );
            }
            sb.append( "<" );
            sb.append( paramType.e.getFamily() );
            sb.append( ">" );
        }
        sb.append( ")" );
        return sb.toString();
    }


    public Consistency getConsistency() {
        return Consistency.NONE;
    }
}

