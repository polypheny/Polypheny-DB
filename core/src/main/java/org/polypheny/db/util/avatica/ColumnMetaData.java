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

package org.polypheny.db.util.avatica;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Type;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata for a column.
 *
 * <p>(Compare with {@link java.sql.ResultSetMetaData}.)
 */
public record ColumnMetaData(
        String label,
        String columnName ) {

    @JsonCreator
    public ColumnMetaData(
            @JsonProperty("label") String label,
            @JsonProperty("columnName") String columnName ) {
        this.label = label;
        // Per the JDBC spec this should be just columnName.
        // For example, the query
        //     select 1 as x, c as y from t
        // should give columns
        //     (label=x, column=null, table=null)
        //     (label=y, column=c table=t)
        // But DbUnit requires every column to have a name. Duh.
        this.columnName = first( columnName, label );
    }


    @Override
    public boolean equals( Object o ) {
        return o == this
                || o instanceof ColumnMetaData
                && Objects.equals( columnName, ((ColumnMetaData) o).columnName );
    }


    private static <T> T first( T t0, T t1 ) {
        return t0 != null ? t0 : t1;
    }



    /**
     * Description of the type used to internally represent a value. For example,
     * a {@link java.sql.Date} might be represented as a {@link #PRIMITIVE_INT}
     * if not nullable, or a {@link #JAVA_SQL_DATE}.
     */
    public enum Rep {
        PRIMITIVE_BOOLEAN( boolean.class, Types.BOOLEAN ),
        PRIMITIVE_BYTE( byte.class, Types.TINYINT ),
        PRIMITIVE_CHAR( char.class, Types.CHAR ),
        PRIMITIVE_SHORT( short.class, Types.SMALLINT ),
        PRIMITIVE_INT( int.class, Types.INTEGER ),
        PRIMITIVE_LONG( long.class, Types.BIGINT ),
        PRIMITIVE_FLOAT( float.class, Types.FLOAT ),
        PRIMITIVE_DOUBLE( double.class, Types.DOUBLE ),
        BOOLEAN( Boolean.class, Types.BOOLEAN ),
        BYTE( Byte.class, Types.TINYINT ),
        CHARACTER( Character.class, Types.CHAR ),
        SHORT( Short.class, Types.SMALLINT ),
        INTEGER( Integer.class, Types.INTEGER ),
        LONG( Long.class, Types.BIGINT ),
        FLOAT( Float.class, Types.FLOAT ),
        DOUBLE( Double.class, Types.DOUBLE ),
        JAVA_SQL_TIME( Time.class, Types.TIME ),
        JAVA_SQL_TIMESTAMP( Timestamp.class, Types.TIMESTAMP ),
        JAVA_SQL_DATE( java.sql.Date.class, Types.DATE ),
        JAVA_UTIL_DATE( java.util.Date.class, Types.DATE ),
        BYTE_STRING( ByteString.class, Types.VARBINARY ),
        STRING( String.class, Types.VARCHAR ),

        /**
         * Values are represented as some subclass of {@link Number}.
         * The JSON encoding does this.
         */
        NUMBER( Number.class, Types.NUMERIC ),

        ARRAY( Array.class, Types.ARRAY ),
        MULTISET( List.class, Types.JAVA_OBJECT ),
        STRUCT( Struct.class, Types.JAVA_OBJECT ),

        OBJECT( Object.class, Types.JAVA_OBJECT );

        public final Class<?> clazz;
        public final int typeId;

        public static final Map<Class<?>, Rep> VALUE_MAP;


        static {
            Map<Class<?>, Rep> builder = new HashMap<>();
            for ( ColumnMetaData.Rep rep : values() ) {
                builder.put( rep.clazz, rep );
            }
            builder.put( byte[].class, BYTE_STRING );
            VALUE_MAP = Collections.unmodifiableMap( builder );
        }


        Rep( Class<?> clazz, int typeId ) {
            this.clazz = clazz;
            this.typeId = typeId;
        }


        public static ColumnMetaData.Rep of( Type clazz ) {
            //noinspection SuspiciousMethodCalls
            final ColumnMetaData.Rep rep = VALUE_MAP.get( clazz );
            return rep != null ? rep : OBJECT;
        }


        /**
         * Returns the value of a column of this type from a result set.
         */
        public Object jdbcGet( ResultSet resultSet, int i ) throws SQLException {
            return switch ( this ) {
                case PRIMITIVE_BOOLEAN -> resultSet.getBoolean( i );
                case PRIMITIVE_BYTE -> resultSet.getByte( i );
                case PRIMITIVE_SHORT -> resultSet.getShort( i );
                case PRIMITIVE_INT -> resultSet.getInt( i );
                case PRIMITIVE_LONG -> resultSet.getLong( i );
                case PRIMITIVE_FLOAT -> resultSet.getFloat( i );
                case PRIMITIVE_DOUBLE -> resultSet.getDouble( i );
                case BOOLEAN -> {
                    final boolean aBoolean = resultSet.getBoolean( i );
                    yield resultSet.wasNull() ? null : aBoolean;
                }
                case BYTE -> {
                    final byte aByte = resultSet.getByte( i );
                    yield resultSet.wasNull() ? null : aByte;
                }
                case SHORT -> {
                    final short aShort = resultSet.getShort( i );
                    yield resultSet.wasNull() ? null : aShort;
                }
                case INTEGER -> {
                    final int anInt = resultSet.getInt( i );
                    yield resultSet.wasNull() ? null : anInt;
                }
                case LONG -> {
                    final long aLong = resultSet.getLong( i );
                    yield resultSet.wasNull() ? null : aLong;
                }
                case FLOAT -> {
                    final float aFloat = resultSet.getFloat( i );
                    yield resultSet.wasNull() ? null : aFloat;
                }
                case DOUBLE -> {
                    final double aDouble = resultSet.getDouble( i );
                    yield resultSet.wasNull() ? null : aDouble;
                }
                case JAVA_SQL_DATE -> resultSet.getDate( i );
                case JAVA_SQL_TIME -> resultSet.getTime( i );
                case JAVA_SQL_TIMESTAMP -> resultSet.getTimestamp( i );
                case ARRAY -> resultSet.getArray( i );
                case STRUCT -> resultSet.getObject( i, Struct.class );
                default -> resultSet.getObject( i );
            };
        }
    }

}
