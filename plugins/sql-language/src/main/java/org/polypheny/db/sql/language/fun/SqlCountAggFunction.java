/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.algebra.fun.CountAggFunction;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * Definition of the SQL <code>COUNT</code> aggregation function.
 *
 * <code>COUNT</code> is an aggregator which returns the number of rows which have gone into it. With one argument (or more), it returns the number of rows for which that argument (or all) is not <code>null</code>.
 */
public class SqlCountAggFunction extends SqlAggFunction implements CountAggFunction {


    public SqlCountAggFunction( String name ) {
        this( name, SqlValidator.STRICT ? OperandTypes.ANY : OperandTypes.ONE_OR_MORE );
    }


    public SqlCountAggFunction( String name, PolyOperandTypeChecker polyOperandTypeChecker ) {
        super(
                name,
                null,
                Kind.COUNT,
                ReturnTypes.BIGINT,
                null,
                polyOperandTypeChecker,
                FunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.FUNCTION_STAR;
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        // Check for COUNT(*) function.  If it is we don't want to try and derive the "*"
        if ( call.isCountStar() ) {
            return validator.getTypeFactory().createPolyType( PolyType.BIGINT );
        }
        return super.deriveType( validator, scope, call );
    }



    @Override
    public FunctionType getFunctionType() {
        return FunctionType.COUNT;
    }

}

