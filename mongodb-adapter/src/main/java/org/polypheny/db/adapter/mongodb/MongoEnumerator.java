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

package org.polypheny.db.adapter.mongodb;


import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.polypheny.db.runtime.functions.Functions;


/**
 * Enumerator that reads from a MongoDB collection.
 */
class MongoEnumerator implements Enumerator<Object> {

    protected final Iterator<Document> cursor;
    protected final Function1<Document, Object> getter;
    protected final GridFSBucket bucket;
    protected Object current;


    /**
     * Creates a MongoEnumerator.
     *
     * @param cursor Mongo iterator (usually a {@link com.mongodb.ServerCursor})
     * @param getter Converts an object into a list of fields
     */
    MongoEnumerator( Iterator<Document> cursor, Function1<Document, Object> getter, GridFSBucket bucket ) {
        this.cursor = cursor;
        this.getter = getter;
        this.bucket = bucket;
    }


    @Override
    public Object current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            if ( cursor.hasNext() ) {
                Document map = cursor.next();
                current = getter.apply( map );

                current = handleTransforms( current );

                return true;
            } else {
                current = null;
                return false;
            }
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    protected Object handleTransforms( Object current ) {
        if ( current == null ) {
            return null;
        }
        if ( current.getClass().isArray() ) {
            List<Object> temp = new ArrayList<>();
            for ( Object el : (Object[]) current ) {
                temp.add( handleTransforms( el ) );
            }
            return temp.toArray();
        } else {
            if ( current instanceof List ) {
                return ((List<?>) current).stream().map( this::handleTransforms ).collect( Collectors.toList() );
            } else if ( current instanceof Document ) {
                return handleDocument( (Document) current );
            }
        }
        return current;
    }


    // s -> stream
    private Object handleDocument( Document el ) {
        if ( el.containsKey( "_type" ) ) {
            String type = el.getString( "_type" );
            if ( type.equals( "s" ) ) {
                // if we have inserted a document and have distributed chunks which we have to fetch
                ObjectId objectId = new ObjectId( (String) ((Document) current).get( "_id" ) );
                GridFSDownloadStream stream = bucket.openDownloadStream( objectId );
                return new PushbackInputStream( stream );
            }
            throw new RuntimeException( "The document type was not recognized" );
        } else {
            return el.toJson();
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        if ( cursor instanceof MongoCursor ) {
            ((MongoCursor) cursor).close();
        }
        // AggregationOutput implements Iterator but not DBCursor. There is no available close() method -- apparently there is no open resource.
    }


    static Function1<Document, Map> mapGetter() {
        return a0 -> (Map) a0;
    }


    /**
     * This method is needed to translate the special types back to their initial ones in Arrays,
     * for example Float is not available in MongoDB and has to be stored as Double,
     * This needs to be fixed when retrieving the arrays.
     * Additionally, for array we cannot be sure how the value is stored, as we lose this information on insert
     */
    static List<Object> arrayGetter( List<Object> objects, Class<?> arrayFieldClass ) {
        if ( arrayFieldClass == Float.class || arrayFieldClass == float.class ) {
            if ( objects.size() > 1 ) {
                if ( objects.get( 0 ) instanceof Double ) {
                    return objects.stream().map( o -> ((Double) o).floatValue() ).collect( Collectors.toList() );
                } else if ( objects.get( 0 ) instanceof Decimal128 ) {
                    return objects.stream().map( obj -> ((Decimal128) obj).floatValue() ).collect( Collectors.toList() );
                }
            }
            return objects;
        } else if ( arrayFieldClass == BigDecimal.class ) {
            return objects.stream().map( obj -> ((Decimal128) obj).bigDecimalValue() ).collect( Collectors.toList() );
        } else if ( arrayFieldClass == double.class ) {
            if ( objects.size() > 1 ) {
                if ( objects.get( 0 ) instanceof Decimal128 ) {
                    return objects.stream().map( o -> ((Decimal128) o).doubleValue() ).collect( Collectors.toList() );
                }
            }
            return objects;
        } else if ( arrayFieldClass == long.class ) {
            if ( objects.size() > 1 ) {
                if ( objects.get( 0 ) instanceof Integer ) {
                    return objects.stream().map( o -> Long.valueOf( (Integer) o ) ).collect( Collectors.toList() );
                }
            }
            return objects;
        } else {
            return objects;
        }
    }


    static Function1<Document, Object> singletonGetter( final String fieldName, final Class<?> fieldClass, Class<?> arrayFieldClass ) {
        return a0 -> {
            Object obj = convert( a0.get( fieldName ), fieldClass );
            if ( fieldClass == List.class ) {
                return arrayGetter( (List) obj, arrayFieldClass );
            }
            return obj;
        };
    }


    /**
     * @param fields List of fields to project; or null to return map
     * @param arrayFields
     */
    static Function1<Document, Object[]> listGetter( final List<Entry<String, Class>> fields, List<Entry<String, Class>> arrayFields ) {
        return a0 -> {
            Object[] objects = new Object[fields.size()];
            for ( int i = 0; i < fields.size(); i++ ) {
                final Map.Entry<String, Class> field = fields.get( i );
                final String name = field.getKey();
                if ( name.equals( "_data" ) ) {
                    objects[i] = Functions.jsonize( a0.get( name ) );
                } else {
                    objects[i] = convert( a0.get( name ), field.getValue() );
                }

                if ( field.getValue() == List.class ) {
                    objects[i] = arrayGetter( (List) objects[i], arrayFields.get( i ).getValue() );
                }
            }
            return objects;
        };
    }


    static Function1<Document, Object> getter( List<Entry<String, Class>> fields, List<Entry<String, Class>> arrayFields ) {
        //noinspection unchecked
        return fields == null
                ? (Function1) mapGetter()
                : fields.size() == 1
                        ? singletonGetter( fields.get( 0 ).getKey(), fields.get( 0 ).getValue(), arrayFields.get( 0 ).getValue() )
                        : (Function1) listGetter( fields, arrayFields );
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
        if ( clazz == BigDecimal.class ) {
            if ( o instanceof Decimal128 ) {
                return ((Decimal128) o).bigDecimalValue();
            } else {
                return BigDecimal.valueOf( (Double) o );
            }
        }

        return o;
    }


}

