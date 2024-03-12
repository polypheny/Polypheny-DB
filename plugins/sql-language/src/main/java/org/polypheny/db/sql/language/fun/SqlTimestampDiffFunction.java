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
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.PolyReturnTypeInference;


/**
 * The <code>TIMESTAMPDIFF</code> function, which calculates the difference between two timestamps.
 *
 * The SQL syntax is
 *
 * <blockquote>
 * <code>TIMESTAMPDIFF(<i>timestamp interval</i>, <i>timestamp</i>, <i>timestamp</i>)</code>
 * </blockquote>
 *
 * The interval time unit can one of the following literals:
 * <ul>
 * <li>NANOSECOND (and synonym SQL_TSI_FRAC_SECOND)</li>
 * <li>MICROSECOND (and synonyms SQL_TSI_MICROSECOND, FRAC_SECOND)</li>
 * <li>SECOND (and synonym SQL_TSI_SECOND)</li>
 * <li>MINUTE (and synonym  SQL_TSI_MINUTE)</li>
 * <li>HOUR (and synonym  SQL_TSI_HOUR)</li>
 * <li>DAY (and synonym SQL_TSI_DAY)</li>
 * <li>WEEK (and synonym  SQL_TSI_WEEK)</li>
 * <li>MONTH (and synonym SQL_TSI_MONTH)</li>
 * <li>QUARTER (and synonym SQL_TSI_QUARTER)</li>
 * <li>YEAR (and synonym  SQL_TSI_YEAR)</li>
 * </ul>
 *
 * Returns difference between two timestamps in indicated timestamp interval.
 */
public class SqlTimestampDiffFunction extends SqlFunction {

    /**
     * Creates a SqlTimestampDiffFunction.
     */
    private static final PolyReturnTypeInference RETURN_TYPE_INFERENCE =
            opBinding -> {
                final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                PolyType polyType = PolyType.BIGINT;
                return typeFactory.createTypeWithNullability(
                        typeFactory.createPolyType( polyType ),
                        opBinding.getOperandType( 1 ).isNullable() || opBinding.getOperandType( 2 ).isNullable() );
            };


    public SqlTimestampDiffFunction() {
        super(
                "TIMESTAMPDIFF",
                Kind.TIMESTAMP_DIFF,
                RETURN_TYPE_INFERENCE,
                null,
                OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.DATETIME, PolyTypeFamily.DATETIME ),
                FunctionCategory.TIMEDATE );
    }

}

