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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;


/**
 * SqlTypeFamily provides SQL type categorization.
 *
 * The <em>primary</em> family categorization is a complete disjoint partitioning of SQL types into families, where two types are members of the same primary family iff instances of the two types can be the operands of an
 * SQL equality predicate such as <code>WHERE v1 = v2</code>. Primary families are returned by RelDataType.getFamily().
 *
 * There is also a <em>secondary</em> family categorization which overlaps with the primary categorization. It is used in type strategies for more specific or more general categorization than the primary families. Secondary
 * families are never returned by RelDataType.getFamily().
 */
public enum SqlTypeFamily implements RelDataTypeFamily {
    // Primary families.
    CHARACTER,
    BINARY,
    NUMERIC,
    DATE,
    TIME,
    TIMESTAMP,
    BOOLEAN,
    INTERVAL_YEAR_MONTH,
    INTERVAL_DAY_TIME,

    // Secondary families.

    STRING,
    APPROXIMATE_NUMERIC,
    EXACT_NUMERIC,
    INTEGER,
    DATETIME,
    DATETIME_INTERVAL,
    MULTISET,
    ARRAY,
    MAP,
    NULL,
    ANY,
    CURSOR,
    COLUMN_LIST,
    GEO;

    private static final Map<Integer, SqlTypeFamily> JDBC_TYPE_TO_FAMILY =
            ImmutableMap.<Integer, SqlTypeFamily>builder()
                    // Not present:
                    // SqlTypeName.MULTISET shares Types.ARRAY with SqlTypeName.ARRAY;
                    // SqlTypeName.MAP has no corresponding JDBC type
                    // SqlTypeName.COLUMN_LIST has no corresponding JDBC type
                    .put( Types.BIT, NUMERIC )
                    .put( Types.TINYINT, NUMERIC )
                    .put( Types.SMALLINT, NUMERIC )
                    .put( Types.BIGINT, NUMERIC )
                    .put( Types.INTEGER, NUMERIC )
                    .put( Types.NUMERIC, NUMERIC )
                    .put( Types.DECIMAL, NUMERIC )

                    .put( Types.FLOAT, NUMERIC )
                    .put( Types.REAL, NUMERIC )
                    .put( Types.DOUBLE, NUMERIC )

                    .put( Types.CHAR, CHARACTER )
                    .put( Types.VARCHAR, CHARACTER )
                    .put( Types.LONGVARCHAR, CHARACTER )
                    .put( Types.CLOB, CHARACTER )

                    .put( Types.BINARY, BINARY )
                    .put( Types.VARBINARY, BINARY )
                    .put( Types.LONGVARBINARY, BINARY )
                    .put( Types.BLOB, BINARY )

                    .put( Types.DATE, DATE )
                    .put( Types.TIME, TIME )
                    .put( ExtraSqlTypes.TIME_WITH_TIMEZONE, TIME )
                    .put( Types.TIMESTAMP, TIMESTAMP )
                    .put( ExtraSqlTypes.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP )
                    .put( Types.BOOLEAN, BOOLEAN )

                    .put( ExtraSqlTypes.REF_CURSOR, CURSOR )
                    .put( Types.ARRAY, ARRAY )
                    .build();


    /**
     * Gets the primary family containing a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     * @return containing family
     */
    public static SqlTypeFamily getFamilyForJdbcType( int jdbcType ) {
        return JDBC_TYPE_TO_FAMILY.get( jdbcType );
    }


    /**
     * @return collection of {@link SqlTypeName}s included in this family
     */
    public Collection<SqlTypeName> getTypeNames() {
        switch ( this ) {
            case CHARACTER:
                return SqlTypeName.CHAR_TYPES;
            case BINARY:
                return SqlTypeName.BINARY_TYPES;
            case NUMERIC:
                return SqlTypeName.NUMERIC_TYPES;
            case DATE:
                return ImmutableList.of( SqlTypeName.DATE );
            case TIME:
                return ImmutableList.of( SqlTypeName.TIME, SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE );
            case TIMESTAMP:
                return ImmutableList.of( SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE );
            case BOOLEAN:
                return SqlTypeName.BOOLEAN_TYPES;
            case INTERVAL_YEAR_MONTH:
                return SqlTypeName.YEAR_INTERVAL_TYPES;
            case INTERVAL_DAY_TIME:
                return SqlTypeName.DAY_INTERVAL_TYPES;
            case STRING:
                return SqlTypeName.STRING_TYPES;
            case APPROXIMATE_NUMERIC:
                return SqlTypeName.APPROX_TYPES;
            case EXACT_NUMERIC:
                return SqlTypeName.EXACT_TYPES;
            case INTEGER:
                return SqlTypeName.INT_TYPES;
            case DATETIME:
                return SqlTypeName.DATETIME_TYPES;
            case DATETIME_INTERVAL:
                return SqlTypeName.INTERVAL_TYPES;
            case GEO:
                return ImmutableList.of( SqlTypeName.GEOMETRY );
            case MULTISET:
                return ImmutableList.of( SqlTypeName.MULTISET );
            case ARRAY:
                return ImmutableList.of( SqlTypeName.ARRAY );
            case MAP:
                return ImmutableList.of( SqlTypeName.MAP );
            case NULL:
                return ImmutableList.of( SqlTypeName.NULL );
            case ANY:
                return SqlTypeName.ALL_TYPES;
            case CURSOR:
                return ImmutableList.of( SqlTypeName.CURSOR );
            case COLUMN_LIST:
                return ImmutableList.of( SqlTypeName.COLUMN_LIST );
            default:
                throw new IllegalArgumentException();
        }
    }


    public boolean contains( RelDataType type ) {
        return SqlTypeUtil.isOfSameTypeName( getTypeNames(), type );
    }
}

