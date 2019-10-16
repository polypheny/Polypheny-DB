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
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperandCountRange;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandCountRanges;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlSingleOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlBasicVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import java.util.Arrays;


/**
 * The dot operator {@code .}, used to access a field of a record. For example, {@code a.b}.
 */
public class SqlDotOperator extends SqlSpecialOperator {

    SqlDotOperator() {
        super( "DOT", SqlKind.DOT, 100, true, null, null, null );
    }


    @Override
    public ReduceResult reduceExpr( int ordinal, TokenSequence list ) {
        SqlNode left = list.node( ordinal - 1 );
        SqlNode right = list.node( ordinal + 1 );
        return new ReduceResult(
                ordinal - 1,
                ordinal + 2,
                createCall(
                        SqlParserPos.sum( Arrays.asList( left.getParserPosition(), right.getParserPosition(), list.pos( ordinal ) ) ),
                        left,
                        right ) );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.IDENTIFIER );
        call.operand( 0 ).unparse( writer, leftPrec, 0 );
        writer.sep( "." );
        call.operand( 1 ).unparse( writer, 0, 0 );
        writer.endList( frame );
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of( 2 );
    }


    @Override
    public <R> void acceptCall( SqlVisitor<R> visitor, SqlCall call, boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler ) {
        if ( onlyExpressions ) {
            // Do not visit operands[1] -- it is not an expression.
            argHandler.visitChild( visitor, call, 0, call.operand( 0 ) );
        } else {
            super.acceptCall( visitor, call, onlyExpressions, argHandler );
        }
    }


    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        final SqlNode operand = call.getOperandList().get( 0 );
        final RelDataType nodeType = validator.deriveType( scope, operand );
        assert nodeType != null;
        if ( !nodeType.isStruct() ) {
            throw SqlUtil.newContextException( operand.getParserPosition(), Static.RESOURCE.incompatibleTypes() );
        }

        final SqlNode fieldId = call.operand( 1 );
        final String fieldName = fieldId.toString();
        final RelDataTypeField field = nodeType.getField( fieldName, false, false );
        if ( field == null ) {
            throw SqlUtil.newContextException( fieldId.getParserPosition(), Static.RESOURCE.unknownField( fieldName ) );
        }
        RelDataType type = field.getType();

        // Validate and determine coercibility and resulting collation name of binary operator if needed.
        type = adjustType( validator, call, type );
        SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType( type );
        return type;
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator() == this;
        // Do not visit call.getOperandList().get(1) here.
        // call.getOperandList().get(1) will be validated when deriveType() is called.
        call.getOperandList().get( 0 ).validateExpr( validator, operandScope );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode left = callBinding.operand( 0 );
        final SqlNode right = callBinding.operand( 1 );
        final RelDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), left );
        if ( type.getSqlTypeName() != SqlTypeName.ROW ) {
            return false;
        } else if ( type.getSqlIdentifier().isStar() ) {
            return false;
        }
        final RelDataType operandType = callBinding.getOperandType( 0 );
        final SqlSingleOperandTypeChecker checker = getChecker( operandType );
        return checker.checkSingleOperandType( callBinding, right, 0, throwOnFailure );
    }


    private SqlSingleOperandTypeChecker getChecker( RelDataType operandType ) {
        switch ( operandType.getSqlTypeName() ) {
            case ROW:
                return OperandTypes.family( SqlTypeFamily.STRING );
            default:
                throw new AssertionError( operandType.getSqlTypeName() );
        }
    }


    @Override
    public boolean validRexOperands( final int count, final Litmus litmus ) {
        return litmus.fail( "DOT is valid only for SqlCall not for RexCall" );
    }


    @Override
    public String getAllowedSignatures( String name ) {
        return "<A>.<B>";
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType recordType = opBinding.getOperandType( 0 );
        switch ( recordType.getSqlTypeName() ) {
            case ROW:
                final String fieldName = opBinding.getOperandLiteralValue( 1, String.class );
                final RelDataType type = opBinding.getOperandType( 0 )
                        .getField( fieldName, false, false )
                        .getType();
                if ( recordType.isNullable() ) {
                    return typeFactory.createTypeWithNullability( type, true );
                } else {
                    return type;
                }
            default:
                throw new AssertionError();
        }
    }
}
