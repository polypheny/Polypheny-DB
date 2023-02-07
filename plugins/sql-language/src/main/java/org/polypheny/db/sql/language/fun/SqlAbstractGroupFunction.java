/*
 * Copyright 2019-2023 The Polypheny Project
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
 */

package org.polypheny.db.sql.language.fun;


import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.validate.AggregatingSelectScope;
import org.polypheny.db.sql.language.validate.OrderByScope;
import org.polypheny.db.sql.language.validate.SelectScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.Static;


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
    public SqlAbstractGroupFunction( String name, Kind kind, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, FunctionCategory category ) {
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
        for ( SqlNode operand : call.getSqlOperandList() ) {
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

