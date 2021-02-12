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

package org.polypheny.db.type;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFamily;


/**
 * SqlTypeFamily provides SQL type categorization.
 * <p>
 * The <em>primary</em> family categorization is a complete disjoint partitioning of SQL types into families, where two types
 * are members of the same primary family iff instances of the two types can be the operands of an SQL equality predicate
 * such as <code>WHERE v1 = v2</code>. Primary families are returned by RelDataType.getFamily().
 * <p>
 * There is also a <em>secondary</em> family categorization which overlaps with the primary categorization. It is used in
 * type strategies for more specific or more general categorization than the primary families. Secondary families are never
 * returned by RelDataType.getFamily().
 */
public enum PolyTypeFamily implements RelDataTypeFamily {
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
    GEO,
    MULTIMEDIA;

    private static final Map<Integer, PolyTypeFamily> JDBC_TYPE_TO_FAMILY =
            ImmutableMap.<Integer, PolyTypeFamily>builder()
                    // Not present:
                    // PolyType.MULTISET shares Types.ARRAY with PolyType.ARRAY;
                    // PolyType.MAP has no corresponding JDBC type
                    // PolyType.COLUMN_LIST has no corresponding JDBC type
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
                    .put( ExtraPolyTypes.TIME_WITH_TIMEZONE, TIME )
                    .put( Types.TIMESTAMP, TIMESTAMP )
                    .put( ExtraPolyTypes.TIMESTAMP_WITH_TIMEZONE, TIMESTAMP )
                    .put( Types.BOOLEAN, BOOLEAN )

                    .put( ExtraPolyTypes.REF_CURSOR, CURSOR )
                    .put( Types.ARRAY, ARRAY )
                    .build();


    /**
     * Gets the primary family containing a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     * @return containing family
     */
    public static PolyTypeFamily getFamilyForJdbcType( int jdbcType ) {
        return JDBC_TYPE_TO_FAMILY.get( jdbcType );
    }


    /**
     * @return collection of {@link PolyType}s included in this family
     */
    public Collection<PolyType> getTypeNames() {
        switch ( this ) {
            case CHARACTER:
                return PolyType.CHAR_TYPES;
            case BINARY:
                return PolyType.BINARY_TYPES;
            case NUMERIC:
                return PolyType.NUMERIC_TYPES;
            case DATE:
                return ImmutableList.of( PolyType.DATE );
            case TIME:
                return ImmutableList.of( PolyType.TIME, PolyType.TIME_WITH_LOCAL_TIME_ZONE );
            case TIMESTAMP:
                return ImmutableList.of( PolyType.TIMESTAMP, PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE );
            case BOOLEAN:
                return PolyType.BOOLEAN_TYPES;
            case INTERVAL_YEAR_MONTH:
                return PolyType.YEAR_INTERVAL_TYPES;
            case INTERVAL_DAY_TIME:
                return PolyType.DAY_INTERVAL_TYPES;
            case STRING:
                return PolyType.STRING_TYPES;
            case APPROXIMATE_NUMERIC:
                return PolyType.APPROX_TYPES;
            case EXACT_NUMERIC:
                return PolyType.EXACT_TYPES;
            case INTEGER:
                return PolyType.INT_TYPES;
            case DATETIME:
                return PolyType.DATETIME_TYPES;
            case DATETIME_INTERVAL:
                return PolyType.INTERVAL_TYPES;
            case GEO:
                return ImmutableList.of( PolyType.GEOMETRY );
            case MULTISET:
                return ImmutableList.of( PolyType.MULTISET );
            case ARRAY:
                //TODO NH: add array types
                return ImmutableList.of( PolyType.ARRAY );
            case MAP:
                return ImmutableList.of( PolyType.MAP );
            case NULL:
                return ImmutableList.of( PolyType.NULL );
            case ANY:
                return PolyType.ALL_TYPES;
            case CURSOR:
                return ImmutableList.of( PolyType.CURSOR );
            case COLUMN_LIST:
                return ImmutableList.of( PolyType.COLUMN_LIST );
            case MULTIMEDIA:
                return ImmutableList.of( PolyType.FILE, PolyType.IMAGE, PolyType.VIDEO, PolyType.SOUND );
            default:
                throw new IllegalArgumentException();
        }
    }


    public boolean contains( RelDataType type ) {
        return PolyTypeUtil.isOfSameTypeName( getTypeNames(), type );
    }
}

