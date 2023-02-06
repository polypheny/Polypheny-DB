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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Generic operator for nodes with internal syntax.
 *
 * If you do not override {@link #getSqlSyntax()} or {@link #unparse(SqlWriter, SqlCall, int, int)}, they will be unparsed using function syntax, {@code F(arg1, arg2, ...)}. This may be OK for operators that never appear in SQL, only as structural elements in an abstract syntax tree.
 *
 * You can use this operator, without creating a sub-class, for non-expression nodes. Validate will validate the arguments, but will not attempt to deduce a type.
 */
public class SqlInternalOperator extends SqlSpecialOperator {


    public SqlInternalOperator( String name, Kind kind ) {
        this( name, kind, 2 );
    }


    public SqlInternalOperator( String name, Kind kind, int prec ) {
        this( name, kind, prec, true, ReturnTypes.ARG0, null, OperandTypes.VARIADIC );
    }


    public SqlInternalOperator( String name, Kind kind, int prec, boolean isLeftAssoc, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                prec,
                isLeftAssoc,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.FUNCTION;
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        return validateOperands( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call );
    }

}

