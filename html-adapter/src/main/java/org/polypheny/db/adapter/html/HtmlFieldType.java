/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.html;


import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;


/**
 * Type of a field in a Web (HTML) table.
 *
 * Usually, and unless specified explicitly in the header row, a field is of type {@link #STRING}. But specifying the field type in the fields makes it easier to write SQL.
 *
 * Trivially modified from CsvFieldType.
 */
enum HtmlFieldType {
    STRING( null, String.class ),
    BOOLEAN( Primitive.BOOLEAN ),
    BYTE( Primitive.BYTE ),
    CHAR( Primitive.CHAR ),
    SHORT( Primitive.SHORT ),
    INT( Primitive.INT ),
    LONG( Primitive.LONG ),
    FLOAT( Primitive.FLOAT ),
    DOUBLE( Primitive.DOUBLE ),
    DATE( null, java.sql.Date.class ),
    TIME( null, java.sql.Time.class ),
    TIMESTAMP( null, java.sql.Timestamp.class );

    private final Primitive primitive;
    private final Class clazz;

    private static final Map<String, HtmlFieldType> MAP;


    static {
        ImmutableMap.Builder<String, HtmlFieldType> builder = ImmutableMap.builder();
        for ( HtmlFieldType value : values() ) {
            builder.put( value.clazz.getSimpleName(), value );

            if ( value.primitive != null ) {
                builder.put( value.primitive.primitiveClass.getSimpleName(), value );
            }
        }
        MAP = builder.build();
    }


    HtmlFieldType( Primitive primitive ) {
        this( primitive, primitive.boxClass );
    }


    HtmlFieldType( Primitive primitive, Class clazz ) {
        this.primitive = primitive;
        this.clazz = clazz;
    }


    public AlgDataType toType( JavaTypeFactory typeFactory ) {
        return typeFactory.createJavaType( clazz );
    }


    public static HtmlFieldType of( String typeString ) {
        return MAP.get( typeString );
    }
}

