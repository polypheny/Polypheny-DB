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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlJsonValueEmptyOrErrorBehavior;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandCountRanges;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import java.util.ArrayList;
import java.util.List;


/**
 * The <code>JSON_VALUE</code> function.
 */
public class SqlJsonValueFunction extends SqlFunction {

    private final boolean returnAny;


    public SqlJsonValueFunction( String name, boolean returnAny ) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                null,
                ( callBinding, returnType, operandTypes ) -> {
                    RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                    for ( int i = 0; i < operandTypes.length; ++i ) {
                        operandTypes[i] = typeFactory.createSqlType( SqlTypeName.ANY );
                    }
                },
                null,
                SqlFunctionCategory.SYSTEM );
        this.returnAny = returnAny;
    }


    @Override
    public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
        List<SqlNode> operandList = new ArrayList<>();
        operandList.add( operands[0] );
        if ( operands[1] == null ) {
            operandList.add( SqlLiteral.createSymbol( SqlJsonValueEmptyOrErrorBehavior.NULL, pos ) );
            operandList.add( SqlLiteral.createNull( pos ) );
        } else {
            operandList.add( operands[1] );
            operandList.add( operands[2] );
        }
        if ( operands[3] == null ) {
            operandList.add( SqlLiteral.createSymbol( SqlJsonValueEmptyOrErrorBehavior.NULL, pos ) );
            operandList.add( SqlLiteral.createNull( pos ) );
        } else {
            operandList.add( operands[3] );
            operandList.add( operands[4] );
        }
        if ( operands.length == 6 && operands[5] != null ) {
            if ( returnAny ) {
                throw new IllegalArgumentException( "illegal returning clause in json_value_any function" );
            }
            operandList.add( operands[5] );
        } else if ( !returnAny ) {
            SqlDataTypeSpec defaultTypeSpec =
                    new SqlDataTypeSpec(
                            new SqlIdentifier( "VARCHAR", pos ),
                            2000,
                            -1,
                            null,
                            null,
                            pos );
            operandList.add( defaultTypeSpec );
        }
        return super.createCall( functionQualifier, pos, operandList.toArray( SqlNode.EMPTY_ARRAY ) );
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between( 5, 6 );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlValidator validator = callBinding.getValidator();
        RelDataType defaultValueOnEmptyType = validator.getValidatedNodeType( callBinding.operand( 2 ) );
        RelDataType defaultValueOnErrorType = validator.getValidatedNodeType( callBinding.operand( 4 ) );
        RelDataType returnType = validator.deriveType( callBinding.getScope(), callBinding.operand( 5 ) );
        if ( !canCastFrom( callBinding, throwOnFailure, defaultValueOnEmptyType, returnType ) ) {
            return false;
        }
        if ( !canCastFrom( callBinding, throwOnFailure, defaultValueOnErrorType, returnType ) ) {
            return false;
        }
        return true;
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        assert opBinding.getOperandCount() == 5 || opBinding.getOperandCount() == 6;
        RelDataType ret;
        if ( opBinding.getOperandCount() == 6 ) {
            ret = opBinding.getOperandType( 5 );
        } else {
            ret = opBinding.getTypeFactory().createSqlType( SqlTypeName.ANY );
        }
        return opBinding.getTypeFactory().createTypeWithNullability( ret, true );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        assert operandsCount == 5 || operandsCount == 6;
        if ( operandsCount == 6 ) {
            return "{0}({1} RETURNING {6} {2} {3} ON EMPTY {4} {5} ON ERROR)";
        }
        return "{0}({1} {2} {3} ON EMPTY {4} {5} ON ERROR)";
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 5 || call.operandCount() == 6;
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        call.operand( 0 ).unparse( writer, 0, 0 );
        if ( !returnAny ) {
            writer.keyword( "RETURNING" );
            call.operand( 5 ).unparse( writer, 0, 0 );
        }
        unparseEnum( writer, call.operand( 1 ) );
        if ( isDefaultLiteral( call.operand( 1 ) ) ) {
            call.operand( 2 ).unparse( writer, 0, 0 );
        }
        writer.keyword( "ON" );
        writer.keyword( "EMPTY" );
        unparseEnum( writer, call.operand( 3 ) );
        if ( isDefaultLiteral( call.operand( 3 ) ) ) {
            call.operand( 4 ).unparse( writer, 0, 0 );
        }
        writer.keyword( "ON" );
        writer.keyword( "ERROR" );
        writer.endFunCall( frame );
    }


    private void unparseEnum( SqlWriter writer, SqlLiteral literal ) {
        writer.keyword( ((Enum) literal.getValue()).name() );
    }


    private boolean isDefaultLiteral( SqlLiteral literal ) {
        return literal.getValueAs( SqlJsonValueEmptyOrErrorBehavior.class ) == SqlJsonValueEmptyOrErrorBehavior.DEFAULT;
    }


    private boolean canCastFrom( SqlCallBinding callBinding, boolean throwOnFailure, RelDataType inType, RelDataType outType ) {
        if ( SqlTypeUtil.canCastFrom( outType, inType, true ) ) {
            return true;
        }
        if ( throwOnFailure ) {
            throw callBinding.newError( Static.RESOURCE.cannotCastValue( inType.toString(), outType.toString() ) );
        }
        return false;
    }
}

