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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.polypheny.db.adapter.mongodb.util.MongoTupleType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyBigDecimal;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Enumerator that reads from a MongoDB collection.
 */
class MongoEnumerator implements Enumerator<PolyValue[]> {

    protected final Iterator<Document> cursor;
    protected final Function1<Document, PolyValue[]> getter;
    protected final GridFSBucket bucket;
    protected PolyValue[] current;


    /**
     * Creates a MongoEnumerator.
     *
     * @param cursor Mongo iterator (usually a {@link com.mongodb.ServerCursor})
     * @param getter Converts an object into a list of fields
     */
    MongoEnumerator( Iterator<Document> cursor, Function1<Document, PolyValue[]> getter, GridFSBucket bucket ) {
        this.cursor = cursor;
        this.getter = getter;
        this.bucket = bucket;
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            if ( cursor.hasNext() ) {
                Document map = cursor.next();
                current = getter.apply( map );

                //current = handleTransforms( current );

                return true;
            } else {
                current = null;
                return false;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    /*protected PolyValue handleTransforms( Bson current ) {
        if ( current == null ) {
            return null;
        }
        if ( current.getClass().isArray() ) {
            List<PolyValue> temp = new ArrayList<>();
            for ( Bson el : (Bson[]) current ) {
                temp.add( handleTransforms( el ) );
            }
            return temp.toArray();
        } else {
            if ( current instanceof List ) {
                return PolyList.of((List<Bson>) current).stream().map( this::handleTransforms ).collect( Collectors.toList() );
            } else if ( current instanceof Document ) {
                return handleDocument( (Document) current );
            }
        }
        return current;
    }*/

    // s -> stream
    /*private PolyValue handleDocument( Document el ) {
        if ( el.containsKey( "_type" ) ) {
            String type = el.getString( "_type" );
            if ( type.equals( "s" ) ) {
                // if we have inserted a document and have distributed chunks which we have to fetch
                ObjectId objectId = new ObjectId( (String) ((Document) current).get( "_id" ) );
                GridFSDownloadStream stream = bucket.openDownloadStream( objectId );
                return new PushbackInputStream( stream );
            }
            throw new GenericRuntimeException( "The document type was not recognized" );
        } else {
            return el.toJson();
        }
    }*/


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        if ( cursor instanceof MongoCursor ) {
            ((MongoCursor<?>) cursor).close();
        }
        // AggregationOutput implements Iterator but not DBCursor. There is no available close() method -- apparently there is no open resource.
    }


    static <E> Function1<Document, E> mapGetter() {
        return a0 -> (E) a0;
    }


    /**
     * This method is needed to translate the special types back to their initial ones in Arrays,
     * for example Float is not available in MongoDB and has to be stored as Double,
     * This needs to be fixed when retrieving the arrays.
     * Additionally, for array we cannot be sure how the value is stored, as we lose this information on insert
     */
    static List<PolyValue> arrayGetter( List<Object> objects, Class<? extends PolyValue> arrayFieldClass ) {
        /*if ( arrayFieldClass == Float.class || arrayFieldClass == float.class ) {
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
        }*/
        return null;
    }


    static Function1<Document, PolyValue> singletonGetter( final MongoTupleType type ) {
        return a0 -> convert( a0.toBsonDocument().get( type.name ), type );
    }


    /**
     *
     */
    static Function1<Document, PolyValue[]> listGetter( final MongoTupleType type ) {
        /*return a0 -> {
            PolyValue[] objects = new PolyValue[fields.size()];
            for ( int i = 0; i < fields.size(); i++ ) {
                final Map.Entry<String, Class<? extends PolyValue>> field = fields.get( i );
                final String name = field.getKey();

                objects[i] = convert( a0.get( name ), field.getValue() );

                if ( field.getValue() == List.class ) {
                    objects[i] = arrayGetter( (List) objects[i], arrayFields.get( i ).getValue() );
                }
            }
            return objects;
        };*/
        List<Function<BsonValue, PolyValue>> trans = new ArrayList<>();
        for ( MongoTupleType sub : type.subs ) {
            trans.add( o -> convert( o.asDocument().get( sub.name ), sub ) );
        }
        return e -> {
            BsonDocument doc = e.toBsonDocument();

            return trans.stream().map( t -> t.apply( doc ) ).toArray( PolyValue[]::new );
        };
    }


    static Function1<Document, PolyValue[]> getter( MongoTupleType tupleType ) {
        return tupleType == null
                ? mapGetter()
                : listGetter( tupleType );
    }


    private static PolyValue convert( BsonValue o, MongoTupleType type ) {
        if ( o == null || o.isNull() ) {
            return new PolyNull();
        }
        switch ( type.type ) {
            case BIGINT:
                return PolyLong.of( o.asNumber().longValue() );
            case INTEGER:
                return PolyInteger.of( o.asNumber().longValue() );
            case VARCHAR:
                return PolyString.of( o.asString().getValue() );
            case DECIMAL:
                return PolyBigDecimal.of( o.asNumber().decimal128Value().bigDecimalValue() );
        }
        throw new NotImplementedException();

        /*if ( o == null ) {
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

        return o;*/
    }


}

