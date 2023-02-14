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

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * SqlSetOperator represents a relational set theory operator (UNION, INTERSECT, MINUS). These are binary operators, but with an extra boolean attribute tacked on for whether to remove duplicates (e.g. UNION ALL does not remove duplicates).
 */
public class SqlSetOperator extends SqlBinaryOperator {

    private final boolean all;


    public SqlSetOperator( String name, Kind kind, int prec, boolean all ) {
        super(
                name,
                kind,
                prec,
                true,
                ReturnTypes.LEAST_RESTRICTIVE,
                null,
                OperandTypes.SET_OP );
        this.all = all;
    }


    public SqlSetOperator( String name, Kind kind, int prec, boolean all, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                prec,
                true,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
        this.all = all;
    }


    public boolean isAll() {
        return all;
    }


    public boolean isDistinct() {
        return !all;
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        validator.validateQuery( call, operandScope, validator.getUnknownType() );
    }

}

