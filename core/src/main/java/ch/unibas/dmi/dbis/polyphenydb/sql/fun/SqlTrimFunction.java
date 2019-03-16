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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SameOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlSingleOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeTransformCascade;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeTransforms;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;


/**
 * Definition of the "TRIM" builtin SQL function.
 */
public class SqlTrimFunction extends SqlFunction {

    protected static final SqlTrimFunction INSTANCE =
            new SqlTrimFunction( "TRIM", SqlKind.TRIM,
                    ReturnTypes.cascade( ReturnTypes.ARG2, SqlTypeTransforms.TO_NULLABLE, SqlTypeTransforms.TO_VARYING ),
                    OperandTypes.and(
                            OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.STRING ),
                            // Arguments 1 and 2 must have same type
                            new SameOperandTypeChecker( 3 ) {
                                @Override
                                protected List<Integer>
                                getOperandList( int operandCount ) {
                                    return ImmutableList.of( 1, 2 );
                                }
                            } ) );


    /**
     * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
     */
    public enum Flag {
        BOTH( 1, 1 ), LEADING( 1, 0 ), TRAILING( 0, 1 );

        private final int left;
        private final int right;


        Flag( int left, int right ) {
            this.left = left;
            this.right = right;
        }


        public int getLeft() {
            return left;
        }


        public int getRight() {
            return right;
        }


        /**
         * Creates a parse-tree node representing an occurrence of this flag at a particular position in the parsed text.
         */
        public SqlLiteral symbol( SqlParserPos pos ) {
            return SqlLiteral.createSymbol( this, pos );
        }
    }


    public SqlTrimFunction( String name, SqlKind kind, SqlTypeTransformCascade returnTypeInference, SqlSingleOperandTypeChecker operandTypeChecker ) {
        super( name, kind, returnTypeInference, null, operandTypeChecker, SqlFunctionCategory.STRING );
    }


    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        assert call.operand( 0 ) instanceof SqlLiteral : call.operand( 0 );
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
        call.operand( 1 ).unparse( writer, leftPrec, rightPrec );
        writer.sep( "FROM" );
        call.operand( 2 ).unparse( writer, leftPrec, rightPrec );
        writer.endFunCall( frame );
    }


    public String getSignatureTemplate( final int operandsCount ) {
        switch ( operandsCount ) {
            case 3:
                return "{0}([BOTH|LEADING|TRAILING] {1} FROM {2})";
            default:
                throw new AssertionError();
        }
    }


    public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
        assert functionQualifier == null;
        switch ( operands.length ) {
            case 1:
                // This variant occurs when someone writes TRIM(string) as opposed to the sugared syntax TRIM(string FROM string).
                operands = new SqlNode[]{
                        Flag.BOTH.symbol( SqlParserPos.ZERO ),
                        SqlLiteral.createCharString( " ", pos ),
                        operands[0]
                };
                break;
            case 3:
                assert operands[0] instanceof SqlLiteral && ((SqlLiteral) operands[0]).getValue() instanceof Flag;
                if ( operands[1] == null ) {
                    operands[1] = SqlLiteral.createCharString( " ", pos );
                }
                break;
            default:
                throw new IllegalArgumentException( "invalid operand count " + Arrays.toString( operands ) );
        }
        return super.createCall( functionQualifier, pos, operands );
    }


    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( !super.checkOperandTypes( callBinding, throwOnFailure ) ) {
            return false;
        }
        switch ( kind ) {
            case TRIM:
                return SqlTypeUtil.isCharTypeComparable(
                        callBinding,
                        ImmutableList.of( callBinding.operand( 1 ), callBinding.operand( 2 ) ),
                        throwOnFailure );
            default:
                return true;
        }
    }
}

