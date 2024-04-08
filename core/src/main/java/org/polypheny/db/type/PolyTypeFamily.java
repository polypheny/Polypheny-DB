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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;


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
public enum PolyTypeFamily implements AlgDataTypeFamily {
    // Primary families.
    CHARACTER,
    BINARY,
    NUMERIC,
    DATE,
    TIME,
    TIMESTAMP,
    BOOLEAN,
    INTERVAL_YEAR_MONTH,
    INTERVAL_TIME,

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
    MULTIMEDIA,
    DOCUMENT,
    PATH,
    GRAPH;

    private static final Map<Integer, PolyTypeFamily> JDBC_TYPE_TO_FAMILY =
            ImmutableMap.<Integer, PolyTypeFamily>builder()
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
        return switch ( this ) {
            case CHARACTER -> PolyType.CHAR_TYPES;
            case BINARY -> PolyType.BINARY_TYPES;
            case NUMERIC -> PolyType.NUMERIC_TYPES;
            case DATE -> ImmutableList.of( PolyType.DATE );
            case TIME -> ImmutableList.of( PolyType.TIME );
            case TIMESTAMP -> ImmutableList.of( PolyType.TIMESTAMP );
            case BOOLEAN -> PolyType.BOOLEAN_TYPES;
            case INTERVAL_YEAR_MONTH -> PolyType.INTERVAL_TYPES;
            case INTERVAL_TIME -> PolyType.INTERVAL_TYPES;
            case STRING -> PolyType.STRING_TYPES;
            case APPROXIMATE_NUMERIC -> PolyType.APPROX_TYPES;
            case EXACT_NUMERIC -> PolyType.EXACT_TYPES;
            case INTEGER -> PolyType.INT_TYPES;
            case DATETIME -> PolyType.DATETIME_TYPES;
            case DATETIME_INTERVAL -> PolyType.INTERVAL_TYPES;
            case GEO -> ImmutableList.of( PolyType.GEOMETRY );
            case MULTISET -> ImmutableList.of( PolyType.MULTISET );
            case ARRAY ->
                //TODO NH: add array types
                    ImmutableList.of( PolyType.ARRAY );
            case MAP -> ImmutableList.of( PolyType.MAP );
            case NULL -> ImmutableList.of( PolyType.NULL );
            case ANY -> PolyType.ALL_TYPES;
            case CURSOR -> ImmutableList.of( PolyType.CURSOR );
            case COLUMN_LIST -> ImmutableList.of( PolyType.COLUMN_LIST );
            case MULTIMEDIA -> ImmutableList.of( PolyType.FILE, PolyType.IMAGE, PolyType.VIDEO, PolyType.AUDIO );
            default -> throw new IllegalArgumentException();
        };
    }


    public boolean contains( AlgDataType type ) {
        return PolyTypeUtil.isOfSameTypeName( getTypeNames(), type );
    }
}

