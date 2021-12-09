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

package org.polypheny.db.adapter.geode.util;


import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geode.pdx.PdxInstance;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.prepare.JavaRecordType;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;


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
    public AlgDataType createStructType( Class type ) {
        final List<AlgDataTypeField> list = new ArrayList<>();
        for ( Field field : type.getDeclaredFields() ) {
            if ( !Modifier.isStatic( field.getModifiers() ) ) {
                // FIXME: watch out for recursion
                final Type fieldType = field.getType();
                list.add( new AlgDataTypeFieldImpl( field.getName(), list.size(), createType( fieldType ) ) );
            }
        }
        return canonize( new JavaRecordType( list, type ) );
    }


    public AlgDataType createPdxType( PdxInstance pdxInstance ) {
        final List<AlgDataTypeField> list = new ArrayList<>();
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

            list.add( new AlgDataTypeFieldImpl( fieldName, list.size(), createType( fieldType ) ) );
        }

        return canonize( new AlgRecordType( list ) );
    }


    // Experimental flattering the nested structures.
    public AlgDataType createPdxType2( PdxInstance pdxInstance ) {
        final List<AlgDataTypeField> list = new ArrayList<>();
        recursiveCreatePdxType( pdxInstance, list, "" );
        return canonize( new AlgRecordType( list ) );
    }


    private void recursiveCreatePdxType( PdxInstance pdxInstance, List<AlgDataTypeField> list, String fieldNamePrefix ) {
        for ( String fieldName : pdxInstance.getFieldNames() ) {
            Object field = pdxInstance.getField( fieldName );
            final Type fieldType = field.getClass();
            if ( fieldType instanceof PdxInstance ) {
                recursiveCreatePdxType( (PdxInstance) field, list, fieldNamePrefix + fieldName + "." );
            } else {
                list.add( new AlgDataTypeFieldImpl( fieldNamePrefix + fieldName, list.size(), createType( fieldType ) ) );
            }
        }
    }

}

