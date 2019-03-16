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
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import java.util.AbstractList;
import java.util.List;


/**
 * Parameter type-checking strategy for a set operator (UNION, INTERSECT, EXCEPT).
 *
 * Both arguments must be records with the same number of fields, and the fields must be union-compatible.
 */
public class SetopOperandTypeChecker implements SqlOperandTypeChecker {

    public boolean isOptional( int i ) {
        return false;
    }


    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        assert callBinding.getOperandCount() == 2 : "setops are binary (for now)";
        final RelDataType[] argTypes = new RelDataType[callBinding.getOperandCount()];
        int colCount = -1;
        final SqlValidator validator = callBinding.getValidator();
        for ( int i = 0; i < argTypes.length; i++ ) {
            final RelDataType argType = argTypes[i] = callBinding.getOperandType( i );
            if ( !argType.isStruct() ) {
                if ( throwOnFailure ) {
                    throw new AssertionError( "setop arg must be a struct" );
                } else {
                    return false;
                }
            }

            // Each operand must have the same number of columns.
            final List<RelDataTypeField> fields = argType.getFieldList();
            if ( i == 0 ) {
                colCount = fields.size();
                continue;
            }

            if ( fields.size() != colCount ) {
                if ( throwOnFailure ) {
                    SqlNode node = callBinding.operand( i );
                    if ( node instanceof SqlSelect ) {
                        node = ((SqlSelect) node).getSelectList();
                    }
                    throw validator.newValidationError( node, Static.RESOURCE.columnCountMismatchInSetop( callBinding.getOperator().getName() ) );
                } else {
                    return false;
                }
            }
        }

        // The columns must be pairwise union compatible. For each column ordinal, form a 'slice' containing the types of the ordinal'th column j.
        for ( int i = 0; i < colCount; i++ ) {
            final int i2 = i;
            final RelDataType type =
                    callBinding.getTypeFactory().leastRestrictive(
                            new AbstractList<RelDataType>() {
                                public RelDataType get( int index ) {
                                    return argTypes[index].getFieldList().get( i2 ).getType();
                                }


                                public int size() {
                                    return argTypes.length;
                                }
                            } );
            if ( type == null ) {
                if ( throwOnFailure ) {
                    SqlNode field = SqlUtil.getSelectListItem( callBinding.operand( 0 ), i );
                    throw validator.newValidationError(
                            field,
                            Static.RESOURCE.columnTypeMismatchInSetop(
                                    i + 1, // 1-based
                                    callBinding.getOperator().getName() ) );
                } else {
                    return false;
                }
            }
        }

        return true;
    }


    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of( 2 );
    }


    public String getAllowedSignatures( SqlOperator op, String opName ) {
        return "{0} " + opName + " {1}";
    }


    public Consistency getConsistency() {
        return Consistency.NONE;
    }
}

