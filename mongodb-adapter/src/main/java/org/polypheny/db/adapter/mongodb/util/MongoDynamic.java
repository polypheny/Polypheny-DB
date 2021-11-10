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
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.core.BsonUtil;
import org.polypheny.db.type.PolyType;


/**
 * Objects of this class represent a blueprint of a dynamic/prepared BsonValue,
 * they initial prepare the correct places to insert later provided dynamic parameters according to the
 * initial provided BsonDocument
 */
public class MongoDynamic {

    private final HashMap<Long, List<DocWrapper>> docHandles = new HashMap<>(); // parent, key,
    private final HashMap<Long, List<ArrayWrapper>> arrayHandles = new HashMap<>(); // parent, index,
    private final HashMap<Long, Function<Object, BsonValue>> transformerMap = new HashMap<>();
    private final GridFSBucket bucket;
    private final BsonDocument document;
    private final HashMap<Long, Boolean> isRegexMap = new HashMap<>();
    private final HashMap<Long, Boolean> isFuncMap = new HashMap<>();
    private final DataContext dataContext;
    private final boolean isProject;


    public MongoDynamic( BsonDocument preDocument, GridFSBucket bucket, DataContext dataContext ) {
        this.dataContext = dataContext;
        this.document = preDocument.clone();
        this.bucket = bucket;
        this.isProject = !document.isEmpty() && document.getFirstKey().equals( "$project" );
        this.document.forEach( ( k, bsonValue ) -> replaceDynamic( bsonValue, this.document, k, true ) );
    }


    /**
     * Recursively steps through the BsonDocument and stores parent of dynamic/prepared parameters in the corresponding maps.
     * This can later be used to efficiently insert multiple parameters, without the need to traverse the tree every time.
     *
     * @param preDocument the BsonDocument, which holds BsonDynamics
     * @param parent the parent of the momentarily observer preDocument
     * @param key the key (BsonDocument) or index (BsonArray) under which the preDocument is retrievable from the parent
     * @param isDoc flag if parent is Document
     */
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


    /**
     * Stores the information needed to replace a BsonDynamic later on.
     *
     * @param index of the corresponding prepared parameter (?3 -> 3)
     * @param doc parent of dynamic
     * @param key key where object is found from parent ( parent is BsonDocument )
     * @param type type of the object itself, to retrieve the correct MongoDB type
     * @param isRegex flag if the BsonDynamic is a regex, which needs to adjusted
     * @param isFunction flag if the BsonDynamic is defined function, which has to be retrieved uniquely
     */
    public void addHandle( long index, BsonDocument doc, String key, PolyType type, Boolean isRegex, Boolean isFunction ) {
        if ( !arrayHandles.containsKey( index ) ) {
            this.transformerMap.put( index, BsonUtil.getBsonTransformer( type, bucket ) );
            this.isRegexMap.put( index, isRegex );
            this.isFuncMap.put( index, isFunction );
            this.docHandles.put( index, new ArrayList<>() );
            this.arrayHandles.put( index, new ArrayList<>() );
        }
        this.docHandles.get( index ).add( new DocWrapper( key, doc ) );
    }


    /**
     * Stores the information needed to replace a BsonDynamic later on.
     *
     * @param index of the corresponding prepared parameter (?3 -> 3)
     * @param array parent of dynamic
     * @param pos position where object is found from parent ( parent is BsonArray )
     * @param type type of the object itself, to retrieve the correct MongoDB type
     * @param isRegex flag if the BsonDynamic is a regex, which needs to adjusted
     * @param isFunction flag if the BsonDynamic is defined function, which has to be retrieved uniquely
     */
    public void addHandle( long index, BsonArray array, int pos, PolyType type, Boolean isRegex, Boolean isFunction ) {
        if ( !arrayHandles.containsKey( index ) ) {
            this.transformerMap.put( index, BsonUtil.getBsonTransformer( type, bucket ) );
            this.isRegexMap.put( index, isRegex );
            this.isFuncMap.put( index, isFunction );
            this.docHandles.put( index, new ArrayList<>() );
            this.arrayHandles.put( index, new ArrayList<>() );
        }
        this.arrayHandles.get( index ).add( new ArrayWrapper( pos, array ) );
    }


    /**
     * Insert operation, which replaces the initially defined dynamic/prepared objects the provided values.
     *
     * @param parameterValues the dynamic parameters
     * @return a final BsonObject with the correct values inserted
     */
    public BsonDocument insert( Map<Long, Object> parameterValues ) {
        for ( Entry<Long, Object> entry : parameterValues.entrySet() ) {
            if ( arrayHandles.containsKey( entry.getKey() ) ) {
                Boolean isRegex = isRegexMap.get( entry.getKey() );
                Boolean isFunction = isFuncMap.get( entry.getKey() );

                if ( isRegex ) {
                    arrayHandles.get( entry.getKey() ).forEach( el -> el.insert( BsonUtil.replaceLikeWithRegex( (String) entry.getValue() ) ) );
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( BsonUtil.replaceLikeWithRegex( (String) entry.getValue() ) ) );
                } else if ( isFunction ) {
                    // function is always part of a document
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( new BsonString( BsonFunctionHelper.getUsedFunction( entry.getValue() ) ) ) );
                } else {
                    Function<Object, BsonValue> transformer = transformerMap.get( entry.getKey() );
                    if ( this.isProject ) {
                        arrayHandles.get( entry.getKey() ).forEach( el -> el.insert( new BsonDocument( "$literal", transformer.apply( entry.getValue() ) ) ) );
                        docHandles.get( entry.getKey() ).forEach( el -> el.insert( new BsonDocument( "$literal", transformer.apply( entry.getValue() ) ) ) );
                    } else {
                        arrayHandles.get( entry.getKey() ).forEach( el -> el.insert( transformer.apply( entry.getValue() ) ) );
                        docHandles.get( entry.getKey() ).forEach( el -> el.insert( transformer.apply( entry.getValue() ) ) );
                    }

                }
            }
        }
        return document.clone();
    }


    /**
     * Fully prepare a batch of multiple prepared rows and transform them into BsonDocuments.
     *
     * @param parameterValues multiple rows of dynamic parameters
     * @param constructor the initial constructor, which holds the blueprint to replace the dynamic parameter
     * @return a list of rows, which can directly be inserted
     */
    public List<? extends WriteModel<Document>> getAll(
            List<Map<Long, Object>> parameterValues,
            Function<Document, ? extends WriteModel<Document>> constructor ) {
        return parameterValues.stream()
                .map( value -> constructor.apply( BsonUtil.asDocument( insert( value ) ) ) )
                .collect( Collectors.toList() );
    }


    public List<Document> getAll( List<Map<Long, Object>> parameterValues ) {
        return parameterValues.stream().map( value -> BsonUtil.asDocument( insert( value ) ) ).collect( Collectors.toList() );
    }


    /**
     * Helper class which holds replace information for a BsonDocument, which has one or multiple dynamic children
     * and defines how the child can be replaced.
     */
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


    /**
     * Helper class which holds replace information for a BsonDocument, which has one or multiple dynamic children
     * and defines how the child can be replaced.
     */
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

}
