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


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlBasicVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlVisitor;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import org.apache.calcite.linq4j.Ord;


/**
 * An operator describing a window function specification.
 *
 * Operands are as follows:
 *
 * <ul>
 * <li>0: name of window function ({@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall})</li>
 * <li>1: window name ({@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral}) or window in-line specification ({@link SqlWindow})</li>
 * </ul>
 */
public class SqlOverOperator extends SqlBinaryOperator {


    public SqlOverOperator() {
        super(
                "OVER",
                SqlKind.OVER,
                20,
                true,
                ReturnTypes.ARG0_FORCE_NULLABLE,
                null,
                OperandTypes.ANY_ANY );
    }


    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator() == this;
        assert call.operandCount() == 2;
        SqlCall aggCall = call.operand( 0 );
        if ( !aggCall.getOperator().isAggregator() ) {
            throw validator.newValidationError( aggCall, RESOURCE.overNonAggregate() );
        }
        final SqlNode window = call.operand( 1 );
        validator.validateWindow( window, scope, aggCall );
    }


    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        // Validate type of the inner aggregate call
        validateOperands( validator, scope, call );

        // Assume the first operand is an aggregate call and derive its type.
        // When we are sure the window is not empty, pass that information to the aggregate's operator return type inference as groupCount=1
        // Otherwise pass groupCount=0 so the agg operator understands the window can be empty
        SqlNode agg = call.operand( 0 );

        if ( !(agg instanceof SqlCall) ) {
            throw new IllegalStateException( "Argument to SqlOverOperator should be SqlCall, got " + agg.getClass() + ": " + agg );
        }

        SqlNode window = call.operand( 1 );
        SqlWindow w = validator.resolveWindow( window, scope, false );

        final int groupCount = w.isAlwaysNonEmpty() ? 1 : 0;
        final SqlCall aggCall = (SqlCall) agg;

        SqlCallBinding opBinding = new SqlCallBinding( validator, scope, aggCall ) {
            @Override
            public int getGroupCount() {
                return groupCount;
            }
        };

        RelDataType ret = aggCall.getOperator().inferReturnType( opBinding );

        // Copied from validateOperands
        ((SqlValidatorImpl) validator).setValidatedNodeType( call, ret );
        ((SqlValidatorImpl) validator).setValidatedNodeType( agg, ret );
        return ret;
    }


    /**
     * Accepts a {@link SqlVisitor}, and tells it to visit each child.
     *
     * @param visitor Visitor
     */
    public <R> void acceptCall( SqlVisitor<R> visitor, SqlCall call, boolean onlyExpressions, SqlBasicVisitor.ArgHandler<R> argHandler ) {
        if ( onlyExpressions ) {
            for ( Ord<SqlNode> operand : Ord.zip( call.getOperandList() ) ) {
                // If the second param is an Identifier then it's supposed to be a name from a window clause and isn't part of the group by check
                if ( operand == null ) {
                    continue;
                }
                if ( operand.i == 1 && operand.e instanceof SqlIdentifier ) {
                    continue;
                }
                argHandler.visitChild( visitor, call, operand.i, operand.e );
            }
        } else {
            super.acceptCall( visitor, call, onlyExpressions, argHandler );
        }
    }
}

