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

import java.sql.Array;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.util.avatica.ColumnMetaData.AvaticaType;

public class Utils {

    private static final Map<Class<?>, Class<?>> BOX;


    private Utils() {
    }


    static {
        BOX = new HashMap<>();
        BOX.put( boolean.class, Boolean.class );
        BOX.put( byte.class, Byte.class );
        BOX.put( char.class, Character.class );
        BOX.put( short.class, Short.class );
        BOX.put( int.class, Integer.class );
        BOX.put( long.class, Long.class );
        BOX.put( float.class, Float.class );
        BOX.put( double.class, Double.class );
    }


    /**
     * Adapts a primitive array into a {@link List}. For example,
     * {@code asList(new double[2])} returns a {@code List&lt;Double&gt;}.
     */
    public static List<?> primitiveList( final Object array ) {
        // REVIEW: A per-type list might be more efficient. (Or might not.)
        return new AbstractList<>() {
            public Object get( int index ) {
                return java.lang.reflect.Array.get( array, index );
            }


            public int size() {
                return java.lang.reflect.Array.getLength( array );
            }
        };
    }


    /**
     * Returns the boxed class. For example, {@code box(int.class)}
     * returns {@code java.lang.Integer}.
     */
    public static Class<?> box( Class<?> clazz ) {
        if ( clazz.isPrimitive() ) {
            return BOX.get( clazz );
        }
        return clazz;
    }


    public static Array createArray( AvaticaType elementType, Iterable<Object> elements ) {
        // Avoid creating a new List if we already have a List
        List<Object> elementList;
        if ( elements instanceof List ) {
            elementList = (List<Object>) elements;
        } else {
            elementList = new ArrayList<>();
            for ( Object element : elements ) {
                elementList.add( element );
            }
        }
        return new ArrayImpl( elementList, elementType );
    }

}
