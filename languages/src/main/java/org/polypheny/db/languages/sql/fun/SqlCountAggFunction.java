/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql.fun;


import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.languages.sql.SqlAggFunction;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlSplittableAggFunction;
import org.polypheny.db.languages.sql.SqlSyntax;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorScope;
import org.polypheny.db.rel.type.RelDataType;
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
public class SqlCountAggFunction extends SqlAggFunction {


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
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        // Check for COUNT(*) function.  If it is we don't want to try and derive the "*"
        if ( call.isCountStar() ) {
            return validator.getTypeFactory().createPolyType( PolyType.BIGINT );
        }
        return super.deriveType( validator, scope, call );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz == SqlSplittableAggFunction.class ) {
            return clazz.cast( SqlSplittableAggFunction.CountSplitter.INSTANCE );
        }
        return super.unwrap( clazz );
    }


    @Override
    public FunctionType getFunctionType() {
        return FunctionType.COUNT;
    }

}

