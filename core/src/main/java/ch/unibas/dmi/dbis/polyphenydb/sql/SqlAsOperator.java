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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlBasicVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.List;


/**
 * The <code>AS</code> operator associates an expression with an alias.
 */
public class SqlAsOperator extends SqlSpecialOperator {

    /**
     * Creates an AS operator.
     */
    public SqlAsOperator() {
        this(
                "AS",
                SqlKind.AS,
                20,
                true,
                ReturnTypes.ARG0,
                InferTypes.RETURN_TYPE,
                OperandTypes.ANY_ANY );
    }


    protected SqlAsOperator(
            String name,
            SqlKind kind,
            int prec,
            boolean leftAssoc,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                prec,
                leftAssoc,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() >= 2;
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        call.operand( 0 ).unparse( writer, leftPrec, getLeftPrec() );
        final boolean needsSpace = true;
        writer.setNeedWhitespace( needsSpace );
        if ( writer.getDialect().allowsAs() ) {
            writer.sep( "AS" );
            writer.setNeedWhitespace( needsSpace );
        }
        call.operand( 1 ).unparse( writer, getRightPrec(), rightPrec );
        if ( call.operandCount() > 2 ) {
            final SqlWriter.Frame frame1 = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE, "(", ")" );
            for ( SqlNode operand : Util.skip( call.getOperandList(), 2 ) ) {
                writer.sep( ",", false );
                operand.unparse( writer, 0, 0 );
            }
            writer.endList( frame1 );
        }
        writer.endList( frame );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        // The base method validates all operands. We override because we don't want to validate the identifier.
        final List<SqlNode> operands = call.getOperandList();
        assert operands.size() == 2;
        assert operands.get( 1 ) instanceof SqlIdentifier;
        operands.get( 0 ).validateExpr( validator, scope );
        SqlIdentifier id = (SqlIdentifier) operands.get( 1 );
        if ( !id.isSimple() ) {
            throw validator.newValidationError( id, Static.RESOURCE.aliasMustBeSimpleIdentifier() );
        }
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
        // special case for AS:  never try to derive type for alias
        RelDataType nodeType = validator.deriveType( scope, call.operand( 0 ) );
        assert nodeType != null;
        return validateOperands( validator, scope, call );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        return call.getOperandMonotonicity( 0 );
    }
}

