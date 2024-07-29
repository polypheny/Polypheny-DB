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

package org.polypheny.db.sql.language.util;

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
import org.polypheny.db.util.ByteString;

/**
 * Description of the type used to internally represent a value. For example,
 * a {@link java.sql.Date} might be represented as a {@link #PRIMITIVE_INT}
 * if not nullable, or a {@link #JAVA_SQL_DATE}.
 */
public enum SqlTypeRepresentation {
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

    public static final Map<Class<?>, SqlTypeRepresentation> VALUE_MAP;


    static {
        Map<Class<?>, SqlTypeRepresentation> builder = new HashMap<>();
        for ( SqlTypeRepresentation rep : values() ) {
            builder.put( rep.clazz, rep );
        }
        builder.put( byte[].class, BYTE_STRING );
        VALUE_MAP = Collections.unmodifiableMap( builder );
    }


    SqlTypeRepresentation( Class<?> clazz, int typeId ) {
        this.clazz = clazz;
        this.typeId = typeId;
    }


    public static SqlTypeRepresentation of( Type clazz ) {
        //noinspection SuspiciousMethodCalls
        final SqlTypeRepresentation rep = VALUE_MAP.get( clazz );
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
