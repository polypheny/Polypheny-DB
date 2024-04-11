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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * The <code>TIMESTAMPADD</code> function, which adds an interval to a datetime (TIMESTAMP, TIME or DATE).
 * <p>
 * The SQL syntax is
 *
 * <blockquote>
 * <code>TIMESTAMPADD(<i>timestamp interval</i>, <i>quantity</i>, <i>datetime</i>)</code>
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
 * Returns modified datetime.
 */
public class SqlTimestampAddFunction extends SqlFunction {

    private static final int MILLISECOND_PRECISION = 3;
    private static final int MICROSECOND_PRECISION = 6;

    private static final PolyReturnTypeInference RETURN_TYPE_INFERENCE =
            opBinding -> {
                final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                return deduceType(
                        typeFactory,
                        (TimeUnit) opBinding.getOperandLiteralValue( 0, PolyType.TIME ).asSymbol().value,
                        opBinding.getOperandType( 1 ),
                        opBinding.getOperandType( 2 ) );
            };


    public static AlgDataType deduceType( AlgDataTypeFactory typeFactory, TimeUnit timeUnit, AlgDataType operandType1, AlgDataType operandType2 ) {
        final AlgDataType type;
        switch ( timeUnit ) {
            case HOUR:
            case MINUTE:
            case SECOND:
            case MILLISECOND:
            case MICROSECOND:
                switch ( timeUnit ) {
                    case MILLISECOND:
                        type = typeFactory.createPolyType( PolyType.TIMESTAMP, MILLISECOND_PRECISION );
                        break;
                    case MICROSECOND:
                        type = typeFactory.createPolyType( PolyType.TIMESTAMP, MICROSECOND_PRECISION );
                        break;
                    default:
                        if ( operandType2.getPolyType() == PolyType.TIME ) {
                            type = typeFactory.createPolyType( PolyType.TIME );
                        } else {
                            type = typeFactory.createPolyType( PolyType.TIMESTAMP );
                        }
                }
                break;
            default:
                type = operandType2;
        }
        return typeFactory.createTypeWithNullability( type, operandType1.isNullable() || operandType2.isNullable() );
    }


    /**
     * Creates a SqlTimestampAddFunction.
     */
    public SqlTimestampAddFunction() {
        super(
                "TIMESTAMPADD",
                Kind.TIMESTAMP_ADD,
                RETURN_TYPE_INFERENCE,
                null,
                OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.INTEGER, PolyTypeFamily.DATETIME ),
                FunctionCategory.TIMEDATE );
    }

}

