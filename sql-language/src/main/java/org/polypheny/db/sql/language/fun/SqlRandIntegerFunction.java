/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>RAND_INTEGER</code> function. There are two overloads:
 *
 * <ul>
 * <li>RAND_INTEGER(bound) returns a random integer between 0 and bound - 1</li>
 * <li>RAND_INTEGER(seed, bound) returns a random integer between 0 and bound - 1, initializing the random number generator with seed on first call</li>
 * </ul>
 */
public class SqlRandIntegerFunction extends SqlFunction {


    public SqlRandIntegerFunction() {
        super(
                "RAND_INTEGER",
                Kind.OTHER_FUNCTION,
                ReturnTypes.INTEGER,
                null,
                OperandTypes.or( OperandTypes.NUMERIC, OperandTypes.NUMERIC_NUMERIC ),
                FunctionCategory.NUMERIC );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.FUNCTION;
    }


    // Plans referencing context variables should never be cached
    @Override
    public boolean isDynamicFunction() {
        return true;
    }

}

