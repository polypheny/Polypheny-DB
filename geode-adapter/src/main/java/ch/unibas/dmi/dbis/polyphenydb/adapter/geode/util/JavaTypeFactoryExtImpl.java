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

package ch.unibas.dmi.dbis.polyphenydb.adapter.geode.util;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFieldImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelRecordType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaRecordType;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFieldImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelRecordType;

import org.apache.geode.pdx.PdxInstance;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Implementation of {@link JavaTypeFactory}.
 *
 * <strong>NOTE: This class is experimental and subject to change/removal without notice</strong>.
 */
public class JavaTypeFactoryExtImpl extends JavaTypeFactoryImpl {

    /**
     * See <a href="http://stackoverflow.com/questions/16966629/what-is-the-difference-between-getfields-and-getdeclaredfields-in-java-reflectio"> the difference between fields and declared fields</a>.
     */
    @Override
    public RelDataType createStructType( Class type ) {
        final List<RelDataTypeField> list = new ArrayList<>();
        for ( Field field : type.getDeclaredFields() ) {
            if ( !Modifier.isStatic( field.getModifiers() ) ) {
                // FIXME: watch out for recursion
                final Type fieldType = field.getType();
                list.add( new RelDataTypeFieldImpl( field.getName(), list.size(), createType( fieldType ) ) );
            }
        }
        return canonize( new JavaRecordType( list, type ) );
    }


    public RelDataType createPdxType( PdxInstance pdxInstance ) {
        final List<RelDataTypeField> list = new ArrayList<>();
        for ( String fieldName : pdxInstance.getFieldNames() ) {
            Object field = pdxInstance.getField( fieldName );
            Type fieldType;
            if ( field == null ) {
                fieldType = String.class;
            } else if ( field instanceof PdxInstance ) {
                // Map Nested PDX structures as String. This relates with GeodeUtils.convert case when clazz is Null.
                fieldType = Map.class;
                // RelDataType boza = createPdxType((PdxInstance) field);
            } else {
                fieldType = field.getClass();
            }

            list.add( new RelDataTypeFieldImpl( fieldName, list.size(), createType( fieldType ) ) );
        }

        return canonize( new RelRecordType( list ) );
    }


    // Experimental flattering the nested structures.
    public RelDataType createPdxType2( PdxInstance pdxInstance ) {
        final List<RelDataTypeField> list = new ArrayList<>();
        recursiveCreatePdxType( pdxInstance, list, "" );
        return canonize( new RelRecordType( list ) );
    }


    private void recursiveCreatePdxType( PdxInstance pdxInstance, List<RelDataTypeField> list, String fieldNamePrefix ) {
        for ( String fieldName : pdxInstance.getFieldNames() ) {
            Object field = pdxInstance.getField( fieldName );
            final Type fieldType = field.getClass();
            if ( fieldType instanceof PdxInstance ) {
                recursiveCreatePdxType( (PdxInstance) field, list, fieldNamePrefix + fieldName + "." );
            } else {
                list.add( new RelDataTypeFieldImpl( fieldNamePrefix + fieldName, list.size(), createType( fieldType ) ) );
            }
        }
    }

}

