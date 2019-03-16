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


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.AggregatingSelectScope;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.OrderByScope;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SelectScope;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;


/**
 * Base class for grouping functions {@code GROUP_ID}, {@code GROUPING_ID}, {@code GROUPING}.
 */
public class SqlAbstractGroupFunction extends SqlAggFunction {

    /**
     * Creates a SqlAbstractGroupFunction.
     *
     * @param name Name of builtin function
     * @param kind kind of operator implemented by function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param category categorization for function
     */
    public SqlAbstractGroupFunction( String name, SqlKind kind, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory category ) {
        super( name, null, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category, false, false, Optionality.FORBIDDEN );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        super.validateCall( call, validator, scope, operandScope );
        final SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope( scope );
        assert selectScope != null;
        final SqlSelect select = selectScope.getNode();
        if ( !validator.isAggregate( select ) ) {
            throw validator.newValidationError( call, Static.RESOURCE.groupingInAggregate( getName() ) );
        }
        final AggregatingSelectScope aggregatingSelectScope = SqlValidatorUtil.getEnclosingAggregateSelectScope( scope );
        if ( aggregatingSelectScope == null ) {
            // We're probably in the GROUP BY clause
            throw validator.newValidationError( call, Static.RESOURCE.groupingInWrongClause( getName() ) );
        }
        for ( SqlNode operand : call.getOperandList() ) {
            if ( scope instanceof OrderByScope ) {
                operand = validator.expandOrderExpr( select, operand );
            } else {
                operand = validator.expand( operand, scope );
            }
            if ( !aggregatingSelectScope.resolved.get().isGroupingExpr( operand ) ) {
                throw validator.newValidationError( operand, Static.RESOURCE.groupingArgument( getName() ) );
            }
        }
    }


    @Override
    public boolean isQuantifierAllowed() {
        return false;
    }


    @Override
    public boolean allowsFilter() {
        return false;
    }
}

