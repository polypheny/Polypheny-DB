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
import org.polypheny.db.algebra.fun.SumEmptyIsZeroAggFunction;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * <code>Sum0</code> is an aggregator which returns the sum of the values which go into it like <code>Sum</code>. It differs in that when no non null values are applied zero is returned instead of null.
 * Can be used along with <code>Count</code> to implement <code>Sum</code>.
 */
public class SqlSumEmptyIsZeroAggFunction extends SqlAggFunction implements SumEmptyIsZeroAggFunction {


    public SqlSumEmptyIsZeroAggFunction() {
        super(
                "$SUM0",
                null,
                Kind.SUM0,
                ReturnTypes.AGG_SUM_EMPTY_IS_ZERO,
                null,
                OperandTypes.NUMERIC,
                FunctionCategory.NUMERIC,
                false,
                false,
                Optionality.FORBIDDEN );
    }


}
