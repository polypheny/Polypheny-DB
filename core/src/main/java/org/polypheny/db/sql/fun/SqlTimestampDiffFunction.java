/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.sql.fun;


import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.SqlReturnTypeInference;
import org.polypheny.db.sql.type.SqlTypeFamily;
import org.polypheny.db.sql.type.SqlTypeName;
import org.apache.calcite.avatica.util.TimeUnit;


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
class SqlTimestampDiffFunction extends SqlFunction {

    /**
     * Creates a SqlTimestampDiffFunction.
     */
    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
            opBinding -> {
                final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                SqlTypeName sqlTypeName =
                        opBinding.getOperandLiteralValue( 0, TimeUnit.class ) == TimeUnit.NANOSECOND
                                ? SqlTypeName.BIGINT
                                : SqlTypeName.INTEGER;
                return typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType( sqlTypeName ),
                        opBinding.getOperandType( 1 ).isNullable() || opBinding.getOperandType( 2 ).isNullable() );
            };


    SqlTimestampDiffFunction() {
        super(
                "TIMESTAMPDIFF",
                SqlKind.TIMESTAMP_DIFF,
                RETURN_TYPE_INFERENCE,
                null,
                OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME ),
                SqlFunctionCategory.TIMEDATE );
    }
}

