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

package org.polypheny.db.adapter.csv;


import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;


/**
 * Type of field in a CSV file.
 * <p>
 * Usually, and unless specified explicitly in the header row, a field is of type {@link #STRING}.
 * But specifying the field type in the header row makes it easier to write SQL.
 */
public enum CsvFieldType {
    STRING( PolyString.class ),
    BOOLEAN( PolyBoolean.class ),
    BYTE( PolyBinary.class ),
    CHAR( PolyString.class ),
    SHORT( PolyInteger.class ),
    INT( PolyInteger.class ),
    LONG( PolyLong.class ),
    FLOAT( PolyFloat.class ),
    DOUBLE( PolyDouble.class ),
    DATE( PolyDate.class ),
    TIME( PolyTime.class ),
    TIMESTAMP( PolyTimestamp.class );

    @NotNull
    private final Class<? extends PolyValue> clazz;
    @NotNull
    private final String simpleName;

    private static final Map<String, CsvFieldType> MAP = new HashMap<>();


    static {
        for ( CsvFieldType value : values() ) {
            MAP.put( value.simpleName, value );
        }
    }


    CsvFieldType( Class<? extends PolyValue> clazz ) {
        this( clazz, clazz.getSimpleName().toLowerCase().replace( "poly", "" ) );
    }


    CsvFieldType( @NotNull Class<? extends PolyValue> clazz, @NotNull String simpleName ) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }


    public static CsvFieldType getCsvFieldType( PolyType type ) {
        return switch ( type ) {
            case BOOLEAN -> CsvFieldType.BOOLEAN;
            case VARBINARY -> CsvFieldType.BYTE;
            case INTEGER -> CsvFieldType.INT;
            case BIGINT -> CsvFieldType.LONG;
            case REAL -> CsvFieldType.FLOAT;
            case DOUBLE -> CsvFieldType.DOUBLE;
            case VARCHAR -> CsvFieldType.STRING;
            case DATE -> CsvFieldType.DATE;
            case TIME -> CsvFieldType.TIME;
            case TIMESTAMP -> CsvFieldType.TIMESTAMP;
            default -> throw new GenericRuntimeException( "Unsupported datatype: " + type.name() );
        };
    }


    public AlgDataType toType( JavaTypeFactory typeFactory ) {
        AlgDataType javaType = typeFactory.createJavaType( clazz );
        AlgDataType sqlType = typeFactory.createPolyType( javaType.getPolyType() );
        return typeFactory.createTypeWithNullability( sqlType, true );
    }


    public static CsvFieldType of( String typeString ) {
        return MAP.get( typeString );
    }
}
