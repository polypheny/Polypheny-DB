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

package org.polypheny.db.adapter.googlesheet;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;


/**
 * Type of field in a Google Sheet. A field is always of type String.
 */
public enum GoogleSheetFieldType {
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

    private static final Map<String, GoogleSheetFieldType> MAP = new HashMap<>();


    static {
        for ( GoogleSheetFieldType value : values() ) {
            MAP.put( value.simpleName, value );
        }
    }


    private final Class clazz;
    private final String simpleName;


    GoogleSheetFieldType( Primitive primitive ) {
        this( primitive.boxClass, primitive.primitiveClass.getSimpleName() );
    }


    GoogleSheetFieldType( Class clazz, String simpleName ) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }


    public static GoogleSheetFieldType getGoogleSheetFieldType( PolyType type ) {
        return switch ( type ) {
            case BOOLEAN -> GoogleSheetFieldType.BOOLEAN;
            case VARBINARY -> GoogleSheetFieldType.BYTE;
            case INTEGER -> GoogleSheetFieldType.INT;
            case BIGINT -> GoogleSheetFieldType.LONG;
            case REAL -> GoogleSheetFieldType.FLOAT;
            case DOUBLE -> GoogleSheetFieldType.DOUBLE;
            case VARCHAR -> GoogleSheetFieldType.STRING;
            case DATE -> GoogleSheetFieldType.DATE;
            case TIME -> GoogleSheetFieldType.TIME;
            case TIMESTAMP -> GoogleSheetFieldType.TIMESTAMP;
            default -> throw new GenericRuntimeException( "Unsupported datatype: " + type.name() );
        };
    }


    public static GoogleSheetFieldType of( String typeString ) {
        return MAP.get( typeString );
    }


    public AlgDataType toType( JavaTypeFactory typeFactory ) {
        AlgDataType javaType = typeFactory.createJavaType( clazz );
        AlgDataType sqlType = typeFactory.createPolyType( javaType.getPolyType() );
        return typeFactory.createTypeWithNullability( sqlType, true );
    }
}
