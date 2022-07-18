/*
 * Copyright 2019-2022 The Polypheny Project
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

import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.PolyType;

import java.util.HashMap;
import java.util.Map;

/**
 * Both of the files are the same, so not much to say here. Copy-paste
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
        switch ( type ) {
            case BOOLEAN:
                return GoogleSheetFieldType.BOOLEAN;
            case VARBINARY:
                return GoogleSheetFieldType.BYTE;
            case INTEGER:
                return GoogleSheetFieldType.INT;
            case BIGINT:
                return GoogleSheetFieldType.LONG;
            case REAL:
                return GoogleSheetFieldType.FLOAT;
            case DOUBLE:
                return GoogleSheetFieldType.DOUBLE;
            case VARCHAR:
                return GoogleSheetFieldType.STRING;
            case DATE:
                return GoogleSheetFieldType.DATE;
            case TIME:
                return GoogleSheetFieldType.TIME;
            case TIMESTAMP:
                return GoogleSheetFieldType.TIMESTAMP;
            default:
                throw new RuntimeException( "Unsupported datatype: " + type.name() );
        }
    }


    public static GoogleSheetFieldType of( String typeString ) {
        return MAP.get( typeString );
    }


    public AlgDataType toType(JavaTypeFactory typeFactory ) {
        AlgDataType javaType = typeFactory.createJavaType( clazz );
        AlgDataType sqlType = typeFactory.createPolyType( javaType.getPolyType() );
        return typeFactory.createTypeWithNullability( sqlType, true );
    }
}
