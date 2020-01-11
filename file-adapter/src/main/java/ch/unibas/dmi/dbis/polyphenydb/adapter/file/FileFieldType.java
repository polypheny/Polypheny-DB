/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.file;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Primitive;


/**
 * Type of a field in a Web (HTML) table.
 *
 * Usually, and unless specified explicitly in the header row, a field is of type {@link #STRING}. But specifying the field type in the fields makes it easier to write SQL.
 *
 * Trivially modified from CsvFieldType.
 */
enum FileFieldType {
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

    private static final Map<String, FileFieldType> MAP;


    static {
        ImmutableMap.Builder<String, FileFieldType> builder = ImmutableMap.builder();
        for ( FileFieldType value : values() ) {
            builder.put( value.clazz.getSimpleName(), value );

            if ( value.primitive != null ) {
                builder.put( value.primitive.primitiveClass.getSimpleName(), value );
            }
        }
        MAP = builder.build();
    }


    FileFieldType( Primitive primitive ) {
        this( primitive, primitive.boxClass );
    }


    FileFieldType( Primitive primitive, Class clazz ) {
        this.primitive = primitive;
        this.clazz = clazz;
    }


    public RelDataType toType( JavaTypeFactory typeFactory ) {
        return typeFactory.createJavaType( clazz );
    }


    public static FileFieldType of( String typeString ) {
        return MAP.get( typeString );
    }
}

