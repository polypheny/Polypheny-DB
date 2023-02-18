/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.poi.ss.usermodel.Cell;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.PolyType;

/**
 * Type of a field in a Excel file.
 * <p>
 * Usually, and unless specified explicitly in the header row, a field is of type {@link #STRING}. But specifying the field type in the header row makes it easier to write SQL.
 */
enum ExcelFieldType {
    STRING( String.class, "string" ),
    BOOLEAN( Primitive.BOOLEAN ),
    BYTE( Primitive.BYTE ),
    CHAR( Primitive.CHAR ),
    SHORT( Primitive.SHORT ),
    INT( Primitive.INT ),
    LONG( Primitive.LONG ),
    FLOAT( Primitive.FLOAT ),
    DOUBLE( Primitive.DOUBLE ),
    DATE( java.sql.Date.class, "date" ),
    TIME( java.sql.Time.class, "time" ),
    TIMESTAMP( java.sql.Timestamp.class, "timestamp" );

    private final Class clazz;
    private final String simpleName;

    private static final Map<String, ExcelFieldType> MAP = new HashMap<>();


    static {
        for ( ExcelFieldType value : values() ) {
            MAP.put( value.simpleName, value );
        }
    }


    ExcelFieldType( Primitive primitive ) {
        this( primitive.boxClass, primitive.primitiveClass.getSimpleName() );
    }


    ExcelFieldType( Class clazz, String simpleName ) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }


    public static ExcelFieldType getExcelFieldType( PolyType type ) {
        switch ( type ) {
            case BOOLEAN:
                return ExcelFieldType.BOOLEAN;
            case VARBINARY:
                return ExcelFieldType.BYTE;
            case INTEGER:
                return ExcelFieldType.INT;
            case BIGINT:
                return ExcelFieldType.LONG;
            case REAL:
                return ExcelFieldType.FLOAT;
            case DOUBLE:
                return ExcelFieldType.DOUBLE;
            case VARCHAR:
                return ExcelFieldType.STRING;
            case DATE:
                return ExcelFieldType.DATE;
            case TIME:
                return ExcelFieldType.TIME;
            case TIMESTAMP:
                return ExcelFieldType.TIMESTAMP;
            default:
                throw new RuntimeException( "Unsupported datatype: " + type.name() );
        }
    }


    public AlgDataType toType( JavaTypeFactory typeFactory ) {
        AlgDataType javaType = typeFactory.createJavaType( clazz );
        AlgDataType sqlType = typeFactory.createPolyType( javaType.getPolyType() );
        return typeFactory.createTypeWithNullability( sqlType, true );
    }


    public static ExcelFieldType of( String typeString ) {
        return MAP.get( typeString );
    }


    public static ExcelFieldType of( Cell cell ) {
        return MAP.get( cell );
    }
}
