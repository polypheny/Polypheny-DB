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
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Strategies for inferring operand types.
 *
 * @see SqlOperandTypeInference
 * @see ReturnTypes
 */
public abstract class InferTypes {

    private InferTypes() {
    }


    /**
     * Operand type-inference strategy where an unknown operand type is derived from the first operand with a known type.
     */
    public static final SqlOperandTypeInference FIRST_KNOWN =
            ( callBinding, returnType, operandTypes ) -> {
                final RelDataType unknownType = callBinding.getValidator().getUnknownType();
                RelDataType knownType = unknownType;
                for ( SqlNode operand : callBinding.operands() ) {
                    knownType = callBinding.getValidator().deriveType( callBinding.getScope(), operand );
                    if ( !knownType.equals( unknownType ) ) {
                        break;
                    }
                }

                // REVIEW jvs 11-Nov-2008:  We can't assert this because SqlAdvisorValidator produces unknown types for incomplete expressions.
                // Maybe we need to distinguish the two kinds of unknown.
                //assert !knownType.equals(unknownType);
                for ( int i = 0; i < operandTypes.length; ++i ) {
                    operandTypes[i] = knownType;
                }
            };

    /**
     * Operand type-inference strategy where an unknown operand type is derived from the call's return type. If the return type is a record, it must have the same number of fields as the number of operands.
     */
    public static final SqlOperandTypeInference RETURN_TYPE =
            ( callBinding, returnType, operandTypes ) -> {
                for ( int i = 0; i < operandTypes.length; ++i ) {
                    operandTypes[i] =
                            returnType.isStruct()
                                    ? returnType.getFieldList().get( i ).getType()
                                    : returnType;
                }
            };

    /**
     * Operand type-inference strategy where an unknown operand type is assumed to be boolean.
     */
    public static final SqlOperandTypeInference BOOLEAN =
            ( callBinding, returnType, operandTypes ) -> {
                RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                for ( int i = 0; i < operandTypes.length; ++i ) {
                    operandTypes[i] = typeFactory.createSqlType( SqlTypeName.BOOLEAN );
                }
            };

    /**
     * Operand type-inference strategy where an unknown operand type is assumed to be VARCHAR(1024).  This is not something which should be used in most cases (especially since the precision is arbitrary),
     * but for IS [NOT] NULL, we don't really care about the type at all, so it's reasonable to use something that every other type can be cast to.
     */
    public static final SqlOperandTypeInference VARCHAR_1024 =
            ( callBinding, returnType, operandTypes ) -> {
                RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                for ( int i = 0; i < operandTypes.length; ++i ) {
                    operandTypes[i] = typeFactory.createSqlType( SqlTypeName.VARCHAR, 1024 );
                }
            };


    /**
     * Returns an {@link SqlOperandTypeInference} that returns a given list of types.
     */
    public static SqlOperandTypeInference explicit( List<RelDataType> types ) {
        return new ExplicitOperandTypeInference( ImmutableList.copyOf( types ) );
    }
}

