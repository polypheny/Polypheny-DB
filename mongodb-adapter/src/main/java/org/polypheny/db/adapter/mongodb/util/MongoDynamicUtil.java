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
 */

package org.polypheny.db.adapter.mongodb.util;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.type.PolyType;

public class MongoDynamicUtil {


    private final HashMap<Long, List<DocWrapper>> docHandles = new HashMap<>(); // parent, key,
    private final HashMap<Long, List<ArrayWrapper>> arrayHandles = new HashMap<>(); // parent, index,
    private final HashMap<Long, Function<Object, BsonValue>> transformerMap = new HashMap<>();
    private final GridFSBucket bucket;
    private final BsonDocument document;
    private final HashMap<Long, Boolean> isRegexMap = new HashMap<>();
    private final HashMap<Long, Boolean> isFuncMap = new HashMap<>();


    public MongoDynamicUtil( BsonDocument preDocument, GridFSBucket bucket ) {
        this.document = preDocument.clone();
        this.bucket = bucket;
        this.document.forEach( ( k, bsonValue ) -> replaceDynamic( bsonValue, this.document, k, true ) );
    }


    public void replaceDynamic( BsonValue preDocument, BsonValue parent, Object key, boolean isDoc ) {
        if ( preDocument.getBsonType() == BsonType.DOCUMENT ) {
            if ( ((BsonDocument) preDocument).containsKey( "_dyn" ) ) {
                // prepared
                BsonValue bsonIndex = ((BsonDocument) preDocument).get( "_dyn" );
                Boolean isRegex = ((BsonDocument) preDocument).get( "_reg" ).asBoolean().getValue();
                Boolean isFunction = ((BsonDocument) preDocument).get( "_func" ).asBoolean().getValue();
                long pos;
                if ( bsonIndex.isInt64() ) {
                    pos = bsonIndex.asInt64().getValue();
                } else {
                    pos = bsonIndex.asInt32().getValue();
                }
                PolyType polyTyp = PolyType.valueOf( ((BsonDocument) preDocument).get( "_type" ).asString().getValue() );

                if ( isDoc ) {
                    addHandle( pos, (BsonDocument) parent, (String) key, polyTyp, isRegex, isFunction );
                } else {
                    addHandle( pos, (BsonArray) parent, (int) key, polyTyp, isRegex, isFunction );
                }

            } else {
                // normal
                ((BsonDocument) preDocument).forEach( ( k, bsonValue ) -> replaceDynamic( bsonValue, preDocument, k, true ) );
            }
        } else if ( preDocument.getBsonType() == BsonType.ARRAY ) {
            int i = 0;
            for ( BsonValue bsonValue : ((BsonArray) preDocument) ) {
                replaceDynamic( bsonValue, preDocument, i, false );
                i++;
            }
        }
    }


    public void addHandle( long index, BsonDocument doc, String key, PolyType type, Boolean isRegex, Boolean isFunction ) {

        if ( !arrayHandles.containsKey( index ) ) {
            this.transformerMap.put( index, MongoTypeUtil.getBsonTransformer( type, bucket ) );
            this.isRegexMap.put( index, isRegex );
            this.isFuncMap.put( index, isFunction );
            this.docHandles.put( index, new ArrayList<>() );
            this.arrayHandles.put( index, new ArrayList<>() );
        }
        this.docHandles.get( index ).add( new DocWrapper( key, doc ) );
    }


    public void addHandle( long index, BsonArray array, int pos, PolyType type, Boolean isRegex, Boolean isFunction ) {
        if ( !arrayHandles.containsKey( index ) ) {
            this.transformerMap.put( index, MongoTypeUtil.getBsonTransformer( type, bucket ) );
            this.isRegexMap.put( index, isRegex );
            this.isFuncMap.put( index, isFunction );
            this.docHandles.put( index, new ArrayList<>() );
            this.arrayHandles.put( index, new ArrayList<>() );
        }
        this.arrayHandles.get( index ).add( new ArrayWrapper( pos, array ) );
    }


    public BsonDocument insert( Map<Long, Object> parameterValues ) {
        for ( Entry<Long, Object> entry : parameterValues.entrySet() ) {
            if ( arrayHandles.containsKey( entry.getKey() ) ) {
                Boolean isRegex = isRegexMap.get( entry.getKey() );
                Boolean isFuntion = isFuncMap.get( entry.getKey() );

                if ( isRegex ) {
                    arrayHandles.get( entry.getKey() ).forEach( el -> el.insert( MongoTypeUtil.replaceRegex( (String) entry.getValue() ) ) );
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( MongoTypeUtil.replaceRegex( (String) entry.getValue() ) ) );
                } else if ( isFuntion ) {
                    // function is always part of a document
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( new BsonString( BsonFunctionHelper.getUsedFunction( entry.getValue() ) ) ) );
                } else {
                    Function<Object, BsonValue> transformer = transformerMap.get( entry.getKey() );
                    arrayHandles.get( entry.getKey() ).forEach( el -> el.insert( transformer.apply( entry.getValue() ) ) );
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( transformer.apply( entry.getValue() ) ) );
                }
            }
        }
        return document.clone();
    }


    public List<? extends WriteModel<Document>> getAll( List<Map<Long, Object>> parameterValues, Function<Document, ? extends WriteModel<Document>> constructor ) {
        return parameterValues.stream().map( value -> constructor.apply( MongoTypeUtil.asDocument( insert( value ) ) ) ).collect( Collectors.toList() );
    }


    public List<Document> getAll( List<Map<Long, Object>> parameterValues ) {
        return parameterValues.stream().map( value -> MongoTypeUtil.asDocument( insert( value ) ) ).collect( Collectors.toList() );
    }


    static class DocWrapper {

        final String key;
        final BsonDocument doc;


        DocWrapper( String key, BsonDocument doc ) {
            this.key = key;
            this.doc = doc;
        }


        public void insert( BsonValue value ) {
            doc.put( key, value );
        }

    }


    static class ArrayWrapper {

        final int index;
        final BsonArray array;


        ArrayWrapper( int index, BsonArray array ) {
            this.index = index;
            this.array = array;
        }


        public void insert( BsonValue value ) {
            array.set( index, value );
        }

    }


    public static List<UpdateManyModel<Document>> zipUpdate( List<Document> left, List<Document> right ) {
        assert left.size() == right.size();
        ArrayList<UpdateManyModel<Document>> res = new ArrayList<>();
        int pos = 0;
        for ( Document document : left ) {
            res.add( new UpdateManyModel<>( document, right.get( pos ) ) );
            pos++;
        }
        return res;
    }

}
