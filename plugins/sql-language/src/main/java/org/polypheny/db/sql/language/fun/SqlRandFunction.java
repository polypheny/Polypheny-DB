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
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>RAND</code> function. There are two overloads:
 *
 * <ul>
 * <li>RAND() returns a random double between 0 and 1</li>
 * <li>RAND(seed) returns a random double between 0 and 1, initializing the random number generator with seed on first call</li>
 * </ul>
 */
public class SqlRandFunction extends SqlFunction {


    public SqlRandFunction() {
        super(
                "RAND",
                Kind.OTHER_FUNCTION,
                ReturnTypes.DOUBLE,
                null,
                OperandTypes.or( OperandTypes.NILADIC, OperandTypes.NUMERIC ),
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

