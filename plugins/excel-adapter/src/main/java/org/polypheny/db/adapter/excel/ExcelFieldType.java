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

package org.polypheny.db.adapter.excel;

import java.util.HashMap;
import java.util.Map;
import org.apache.poi.ss.usermodel.Cell;
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
 * Type of a field in an Excel file.
 * <p>
 * Usually, and unless specified explicitly in the header row, a field is of type {@link #STRING}.
 * But specifying the field type in the header row makes it easier to write SQL.
 */
public enum ExcelFieldType {
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

    private static final Map<String, ExcelFieldType> MAP = new HashMap<>();


    static {
        for ( ExcelFieldType value : values() ) {
            MAP.put( value.simpleName, value );
        }
    }


    ExcelFieldType( Class<? extends PolyValue> clazz ) {
        this( clazz, clazz.getSimpleName().toLowerCase().replace( "poly", "" ) );
    }


    ExcelFieldType( @NotNull Class<? extends PolyValue> clazz, @NotNull String simpleName ) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }


    public static ExcelFieldType getExcelFieldType( PolyType type ) {
        return switch ( type ) {
            case BOOLEAN -> ExcelFieldType.BOOLEAN;
            case VARBINARY -> ExcelFieldType.BYTE;
            case INTEGER -> ExcelFieldType.INT;
            case BIGINT -> ExcelFieldType.LONG;
            case REAL -> ExcelFieldType.FLOAT;
            case DOUBLE -> ExcelFieldType.DOUBLE;
            case VARCHAR -> ExcelFieldType.STRING;
            case DATE -> ExcelFieldType.DATE;
            case TIME -> ExcelFieldType.TIME;
            case TIMESTAMP -> ExcelFieldType.TIMESTAMP;
            default -> throw new GenericRuntimeException( "Unsupported datatype: " + type.name() );
        };
    }


    public AlgDataType toType( JavaTypeFactory typeFactory ) {
        AlgDataType javaType = typeFactory.createJavaType( clazz );
        AlgDataType sqlType = typeFactory.createPolyType( javaType.getPolyType() );
        return typeFactory.createTypeWithNullability( sqlType, true );
    }


    public static ExcelFieldType of( Cell cell ) {
        return MAP.get( cell );
    }
}
