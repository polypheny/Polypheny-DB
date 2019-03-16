/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import org.apache.calcite.avatica.util.TimeUnit;


/**
 * The <code>TIMESTAMPADD</code> function, which adds an interval to a datetime (TIMESTAMP, TIME or DATE).
 *
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

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
            opBinding -> {
                final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                return deduceType(
                        typeFactory,
                        opBinding.getOperandLiteralValue( 0, TimeUnit.class ),
                        opBinding.getOperandType( 1 ),
                        opBinding.getOperandType( 2 ) );
            };


    public static RelDataType deduceType( RelDataTypeFactory typeFactory, TimeUnit timeUnit, RelDataType operandType1, RelDataType operandType2 ) {
        final RelDataType type;
        switch ( timeUnit ) {
            case HOUR:
            case MINUTE:
            case SECOND:
            case MILLISECOND:
            case MICROSECOND:
                switch ( timeUnit ) {
                    case MILLISECOND:
                        type = typeFactory.createSqlType( SqlTypeName.TIMESTAMP, MILLISECOND_PRECISION );
                        break;
                    case MICROSECOND:
                        type = typeFactory.createSqlType( SqlTypeName.TIMESTAMP, MICROSECOND_PRECISION );
                        break;
                    default:
                        if ( operandType2.getSqlTypeName() == SqlTypeName.TIME ) {
                            type = typeFactory.createSqlType( SqlTypeName.TIME );
                        } else {
                            type = typeFactory.createSqlType( SqlTypeName.TIMESTAMP );
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
    SqlTimestampAddFunction() {
        super(
                "TIMESTAMPADD",
                SqlKind.TIMESTAMP_ADD,
                RETURN_TYPE_INFERENCE,
                null,
                OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME ),
                SqlFunctionCategory.TIMEDATE );
    }
}

