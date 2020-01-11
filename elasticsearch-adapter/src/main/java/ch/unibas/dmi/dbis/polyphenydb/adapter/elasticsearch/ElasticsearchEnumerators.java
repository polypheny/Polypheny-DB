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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;


/**
 * Util functions which convert {@link ElasticsearchJson.SearchHit} into Polypheny-DB specific return type (map, object[], list etc.)
 */
class ElasticsearchEnumerators {

    private ElasticsearchEnumerators() {
    }


    private static Function1<ElasticsearchJson.SearchHit, Map> mapGetter() {
        return ElasticsearchJson.SearchHit::sourceOrFields;
    }


    private static Function1<ElasticsearchJson.SearchHit, Object> singletonGetter( final String fieldName, final Class fieldClass, final Map<String, String> mapping ) {
        return hit -> {
            final String key;
            if ( hit.sourceOrFields().containsKey( fieldName ) ) {
                key = fieldName;
            } else {
                key = mapping.getOrDefault( fieldName, fieldName );
            }

            final Object value;
            if ( ElasticsearchConstants.ID.equals( key ) || ElasticsearchConstants.ID.equals( mapping.getOrDefault( fieldName, fieldName ) ) ) {
                // is the original projection on _id field ?
                value = hit.id();
            } else {
                value = hit.valueOrNull( key );
            }
            return convert( value, fieldClass );
        };
    }


    /**
     * Function that extracts a given set of fields from elastic search result objects.
     *
     * @param fields List of fields to project
     * @return function that converts the search result into a generic array
     */
    private static Function1<ElasticsearchJson.SearchHit, Object[]> listGetter( final List<Map.Entry<String, Class>> fields, Map<String, String> mapping ) {
        return hit -> {
            Object[] objects = new Object[fields.size()];
            for ( int i = 0; i < fields.size(); i++ ) {
                final Map.Entry<String, Class> field = fields.get( i );
                final String key;
                if ( hit.sourceOrFields().containsKey( field.getKey() ) ) {
                    key = field.getKey();
                } else {
                    key = mapping.getOrDefault( field.getKey(), field.getKey() );
                }

                final Object value;
                if ( ElasticsearchConstants.ID.equals( key )
                        || ElasticsearchConstants.ID.equals( mapping.get( field.getKey() ) )
                        || ElasticsearchConstants.ID.equals( field.getKey() ) ) {
                    // is the original projection on _id field ?
                    value = hit.id();
                } else {
                    value = hit.valueOrNull( key );
                }

                final Class type = field.getValue();
                objects[i] = convert( value, type );
            }
            return objects;
        };
    }


    static Function1<ElasticsearchJson.SearchHit, Object> getter( List<Map.Entry<String, Class>> fields, Map<String, String> mapping ) {
        //noinspection unchecked
        final Function1 getter;
        if ( fields == null || fields.size() == 1 && "_MAP".equals( fields.get( 0 ).getKey() ) ) {
            // select * from table
            getter = mapGetter();
        } else if ( fields.size() == 1 ) {
            // select foo from table
            getter = singletonGetter( fields.get( 0 ).getKey(), fields.get( 0 ).getValue(), mapping );
        } else {
            // select a, b, c from table
            getter = listGetter( fields, mapping );
        }

        return getter;
    }


    private static Object convert( Object o, Class clazz ) {
        if ( o == null ) {
            return null;
        }
        Primitive primitive = Primitive.of( clazz );
        if ( primitive != null ) {
            clazz = primitive.boxClass;
        } else {
            primitive = Primitive.ofBox( clazz );
        }
        if ( clazz.isInstance( o ) ) {
            return o;
        }
        if ( o instanceof Date && primitive != null ) {
            o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
        }
        if ( o instanceof Number && primitive != null ) {
            return primitive.number( (Number) o );
        }
        return o;
    }
}

