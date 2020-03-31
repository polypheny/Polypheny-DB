/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.sql.fun;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Static;


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
                        operandTypes[i] = typeFactory.createPolyType( PolyType.ANY );
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
        return PolyOperandCountRanges.between( 5, 6 );
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
            ret = opBinding.getTypeFactory().createPolyType( PolyType.ANY );
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
        if ( PolyTypeUtil.canCastFrom( outType, inType, true ) ) {
            return true;
        }
        if ( throwOnFailure ) {
            throw callBinding.newError( Static.RESOURCE.cannotCastValue( inType.toString(), outType.toString() ) );
        }
        return false;
    }
}

