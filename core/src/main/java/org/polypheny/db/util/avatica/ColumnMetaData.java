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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.lang.reflect.Type;
import java.sql.Array;
import java.sql.DatabaseMetaData;
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
import lombok.Getter;

/**
 * Metadata for a column.
 *
 * <p>(Compare with {@link java.sql.ResultSetMetaData}.)
 *
 * @param ordinal 0-based
 */
public record ColumnMetaData(
        int ordinal,
        boolean autoIncrement,
        boolean caseSensitive,
        boolean searchable,
        boolean currency,
        int nullable,
        boolean signed,
        int displaySize,
        String label,
        String columnName,
        String schemaName,
        int precision,
        int scale,
        String tableName,
        String catalogName,
        ColumnMetaData.AvaticaType type,
        boolean readOnly,
        boolean writable,
        boolean definitelyWritable,
        String columnClassName
) {

    @JsonCreator
    public ColumnMetaData(
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("autoIncrement") boolean autoIncrement,
            @JsonProperty("caseSensitive") boolean caseSensitive,
            @JsonProperty("searchable") boolean searchable,
            @JsonProperty("currency") boolean currency,
            @JsonProperty("nullable") int nullable,
            @JsonProperty("signed") boolean signed,
            @JsonProperty("displaySize") int displaySize,
            @JsonProperty("label") String label,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("precision") int precision,
            @JsonProperty("scale") int scale,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("type") ColumnMetaData.AvaticaType type,
            @JsonProperty("readOnly") boolean readOnly,
            @JsonProperty("writable") boolean writable,
            @JsonProperty("definitelyWritable") boolean definitelyWritable,
            @JsonProperty("columnClassName") String columnClassName ) {
        this.ordinal = ordinal;
        this.autoIncrement = autoIncrement;
        this.caseSensitive = caseSensitive;
        this.searchable = searchable;
        this.currency = currency;
        this.nullable = nullable;
        this.signed = signed;
        this.displaySize = displaySize;
        this.label = label;
        // Per the JDBC spec this should be just columnName.
        // For example, the query
        //     select 1 as x, c as y from t
        // should give columns
        //     (label=x, column=null, table=null)
        //     (label=y, column=c table=t)
        // But DbUnit requires every column to have a name. Duh.
        this.columnName = first( columnName, label );
        this.schemaName = schemaName;
        this.precision = precision;
        this.scale = scale;
        this.tableName = tableName;
        this.catalogName = catalogName;
        this.type = type;
        this.readOnly = readOnly;
        this.writable = writable;
        this.definitelyWritable = definitelyWritable;
        this.columnClassName = columnClassName;
    }


    @Override
    public boolean equals( Object o ) {
        return o == this
                || o instanceof ColumnMetaData
                && autoIncrement == ((ColumnMetaData) o).autoIncrement
                && caseSensitive == ((ColumnMetaData) o).caseSensitive
                && Objects.equals( catalogName, ((ColumnMetaData) o).catalogName )
                && Objects.equals( columnClassName, ((ColumnMetaData) o).columnClassName )
                && Objects.equals( columnName, ((ColumnMetaData) o).columnName )
                && currency == ((ColumnMetaData) o).currency
                && definitelyWritable == ((ColumnMetaData) o).definitelyWritable
                && displaySize == ((ColumnMetaData) o).displaySize
                && Objects.equals( label, ((ColumnMetaData) o).label )
                && nullable == ((ColumnMetaData) o).nullable
                && ordinal == ((ColumnMetaData) o).ordinal
                && precision == ((ColumnMetaData) o).precision
                && readOnly == ((ColumnMetaData) o).readOnly
                && scale == ((ColumnMetaData) o).scale
                && Objects.equals( schemaName, ((ColumnMetaData) o).schemaName )
                && searchable == ((ColumnMetaData) o).searchable
                && signed == ((ColumnMetaData) o).signed
                && Objects.equals( tableName, ((ColumnMetaData) o).tableName )
                && Objects.equals( type, ((ColumnMetaData) o).type )
                && writable == ((ColumnMetaData) o).writable;
    }


    private static <T> T first( T t0, T t1 ) {
        return t0 != null ? t0 : t1;
    }


    /**
     * Creates a {@link ColumnMetaData.ScalarType}.
     */
    public static ColumnMetaData.ScalarType scalar( int type, String typeName, ColumnMetaData.Rep rep ) {
        return new ColumnMetaData.ScalarType( type, typeName, rep );
    }


    /**
     * Creates a {@link ColumnMetaData.StructType}.
     */
    public static ColumnMetaData.StructType struct( List<ColumnMetaData> columns ) {
        return new ColumnMetaData.StructType( columns );
    }


    /**
     * Creates an {@link ColumnMetaData.ArrayType}.
     */
    public static ColumnMetaData.ArrayType array(
            ColumnMetaData.AvaticaType componentType, String typeName,
            ColumnMetaData.Rep rep ) {
        return new ColumnMetaData.ArrayType( Types.ARRAY, typeName, rep, componentType );
    }


    /**
     * Creates a ColumnMetaData for result sets that are not based on a struct
     * but need to have a single 'field' for purposes of
     * {@link java.sql.ResultSetMetaData}.
     */
    public static ColumnMetaData dummy( ColumnMetaData.AvaticaType type, boolean nullable ) {
        return new ColumnMetaData(
                0,
                false,
                true,
                false,
                false,
                nullable
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true,
                -1,
                null,
                null,
                null,
                -1,
                -1,
                null,
                null,
                type,
                true,
                false,
                false,
                type.columnClassName() );
    }


    public ColumnMetaData setRep( ColumnMetaData.Rep rep ) {
        return new ColumnMetaData( ordinal, autoIncrement, caseSensitive, searchable,
                currency, nullable, signed, displaySize, label, columnName, schemaName,
                precision, scale, tableName, catalogName, type.setRep( rep ), readOnly,
                writable, definitelyWritable, columnClassName );
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
         * Values are represented as some sub-class of {@link Number}.
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


    /**
     * Base class for a column type.
     */
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "type",
            defaultImpl = ColumnMetaData.ScalarType.class)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ColumnMetaData.ScalarType.class, name = "scalar"),
            @JsonSubTypes.Type(value = ColumnMetaData.StructType.class, name = "struct"),
            @JsonSubTypes.Type(value = ColumnMetaData.ArrayType.class, name = "array") })
    public static class AvaticaType {

        public final int id;
        @Getter
        public final String name;

        /**
         * The type of the field that holds the value. Not a JDBC property.
         */
        public final ColumnMetaData.Rep rep;


        public AvaticaType( int id, String name, ColumnMetaData.Rep rep ) {
            this.id = id;
            this.name = Objects.requireNonNull( name );
            this.rep = Objects.requireNonNull( rep );
        }


        public String columnClassName() {
            return SqlType.valueOf( id ).boxedClass().getName();
        }


        public ColumnMetaData.AvaticaType setRep( ColumnMetaData.Rep rep ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public int hashCode() {
            return Objects.hash( id, name, rep );
        }


        @Override
        public boolean equals( Object o ) {
            return o == this
                    || o instanceof ColumnMetaData.AvaticaType
                    && id == ((ColumnMetaData.AvaticaType) o).id
                    && Objects.equals( name, ((ColumnMetaData.AvaticaType) o).name )
                    && rep == ((ColumnMetaData.AvaticaType) o).rep;
        }

    }


    /**
     * Scalar type.
     */
    public static class ScalarType extends ColumnMetaData.AvaticaType {

        @JsonCreator
        public ScalarType(
                @JsonProperty("id") int id,
                @JsonProperty("name") String name,
                @JsonProperty("rep") ColumnMetaData.Rep rep ) {
            super( id, name, rep );
        }


        @Override
        public ColumnMetaData.AvaticaType setRep( ColumnMetaData.Rep rep ) {
            return new ColumnMetaData.ScalarType( id, name, rep );
        }

    }


    /**
     * Record type.
     */
    public static class StructType extends ColumnMetaData.AvaticaType {

        public final List<ColumnMetaData> columns;


        @JsonCreator
        public StructType( List<ColumnMetaData> columns ) {
            super( Types.STRUCT, "STRUCT", ColumnMetaData.Rep.OBJECT );
            this.columns = columns;
        }


        @Override
        public int hashCode() {
            return Objects.hash( id, name, rep, columns );
        }


        @Override
        public boolean equals( Object o ) {
            return o == this
                    || o instanceof ColumnMetaData.StructType
                    && super.equals( o )
                    && Objects.equals( columns, ((ColumnMetaData.StructType) o).columns );
        }

    }


    /**
     * Array type.
     */
    @Getter
    public static class ArrayType extends ColumnMetaData.AvaticaType {

        private final ColumnMetaData.AvaticaType component;


        /**
         * Not for public use. Use {@link ColumnMetaData#array(ColumnMetaData.AvaticaType, String, ColumnMetaData.Rep)}.
         */
        @JsonCreator
        public ArrayType(
                @JsonProperty("type") int type, @JsonProperty("name") String typeName,
                @JsonProperty("rep") ColumnMetaData.Rep representation, @JsonProperty("component") ColumnMetaData.AvaticaType component ) {
            super( type, typeName, representation );
            this.component = component;
        }


        @Override
        public int hashCode() {
            return Objects.hash( id, name, rep, component );
        }


        @Override
        public boolean equals( Object o ) {
            return o == this
                    || o instanceof ColumnMetaData.ArrayType
                    && super.equals( o )
                    && Objects.equals( component, ((ColumnMetaData.ArrayType) o).component );
        }

    }

}

// End ColumnMetaData.java
