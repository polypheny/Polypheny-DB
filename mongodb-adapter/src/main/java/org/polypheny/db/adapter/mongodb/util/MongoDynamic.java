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
 */

package org.polypheny.db.adapter.mongodb.util;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Setter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BsonUtil;


/**
 * Objects of this class represent a blueprint of a dynamic/prepared BsonValue,
 * they initial prepare the correct places to insert later provided dynamic parameters according to the
 * initial provided BsonDocument
 */
public class MongoDynamic {

    private final Map<Long, List<DocWrapper>> docHandles = new HashMap<>(); // parent, key,
    private final Map<Long, List<ArrayWrapper>> arrayHandles = new HashMap<>(); // parent, index,

    private final Map<Long, List<KeyWrapper>> keyHandles = new HashMap<>(); // parent, index,
    private final Map<Long, Function<Object, BsonValue>> transformerMap = new HashMap<>();
    private final GridFSBucket bucket;
    private final BsonDocument document;
    private final Map<Long, Boolean> isRegexMap = new HashMap<>();
    private final Map<Long, Boolean> isFuncMap = new HashMap<>();
    private final DataContext dataContext;
    private final boolean isProject;
    private final Map<Long, Boolean> isValueMap = new HashMap<>();

    private final Map<Long, String> keyMap = new HashMap<>();


    public MongoDynamic( BsonDocument document, GridFSBucket bucket, DataContext dataContext ) {
        this.dataContext = dataContext;
        this.document = document.clone();
        this.bucket = bucket;
        this.isProject = !document.isEmpty() && document.getFirstKey().equals( "$project" );
        this.document.forEach( ( k, bsonValue ) -> replaceDynamic( bsonValue, this.document, k, true, false ) );
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
    public void replaceDynamic( BsonValue preDocument, BsonValue parent, Object key, boolean isDoc, boolean isKey ) {
        if ( preDocument.getBsonType() == BsonType.DOCUMENT ) {
            if ( ((BsonDocument) preDocument).containsKey( "_dyn" ) ) {
                // prepared
                handleDynamic( (BsonDocument) preDocument, parent, key, isDoc, isKey );
            } else if ( isDynamic( preDocument ) ) {
                String keyChange = preDocument.asDocument().getFirstKey();

                // handle value
                replaceDynamic( preDocument.asDocument().get( preDocument.asDocument().getFirstKey() ), preDocument, ((BsonDocument) preDocument).getFirstKey(), true, false );
                // handle changed key after
                handleDynamic( BsonDocument.parse( keyChange ), preDocument, 0, false, true );
            } else {
                // normal
                ((BsonDocument) preDocument).forEach( ( k, bsonValue ) -> replaceDynamic( bsonValue, preDocument, k, true, false ) );
            }
        } else if ( preDocument.getBsonType() == BsonType.ARRAY ) {
            int i = 0;
            for ( BsonValue bsonValue : ((BsonArray) preDocument) ) {
                replaceDynamic( bsonValue, preDocument, i, false, false );
                i++;
            }
        }
    }


    private void handleDynamic( BsonDocument preDocument, BsonValue parent, Object key, boolean isDoc, boolean isKey ) {
        BsonValue bsonIndex = preDocument.get( "_dyn" );
        Boolean isRegex = preDocument.get( "_reg" ).asBoolean().getValue();
        Boolean isFunction = preDocument.get( "_func" ).asBoolean().getValue();
        Boolean isValue = preDocument.get( "_isVal" ).asBoolean().getValue();
        String keyName = preDocument.get( "_key" ).asString().getValue();
        long pos;
        if ( bsonIndex.isInt64() ) {
            pos = bsonIndex.asInt64().getValue();
        } else {
            pos = bsonIndex.asInt32().getValue();
        }
        Queue<PolyType> polyTypes = Arrays.stream( preDocument.get( "_type" ).asString().getValue().split( "\\$" ) ).map( PolyType::valueOf ).collect( Collectors.toCollection( LinkedList::new ) );

        if ( isDoc ) {
            addDocHandle( pos, (BsonDocument) parent, (String) key, polyTypes, isRegex, isFunction, isValue, keyName );
        } else if ( isKey ) {
            addKeyHandle( pos, (BsonDocument) parent, polyTypes, isRegex, isFunction, isValue, keyName );
        } else {
            addArrayHandle( pos, (BsonArray) parent, (int) key, polyTypes, isRegex, isFunction, isValue, keyName );
        }
    }


    private boolean isDynamic( BsonValue preDocument ) {
        if ( preDocument.asDocument().size() != 1 ) {
            return false;
        }
        String key = preDocument.asDocument().getFirstKey();
        if ( !key.startsWith( "{" ) || !key.endsWith( "}" ) ) {
            return false;
        }

        try {
            BsonDynamic.parse( key );
            return true;
        } catch ( Exception e ) {
            return false;
        }
    }


    /**
     * Stores the information needed to replace a BsonDynamic later on.
     *
     * @param index of the corresponding prepared parameter (?3 -> 3)
     * @param doc parent of dynamic
     * @param key key where object is found from parent ( parent is BsonDocument )
     * @param types type of the object itself, to retrieve the correct MongoDB type
     * @param isRegex flag if the BsonDynamic is a regex, which needs to adjusted
     * @param isFunction flag if the BsonDynamic is defined function, which has to be retrieved uniquely
     * @param isValue
     */
    public void addDocHandle( long index, BsonDocument doc, String key, Queue<PolyType> types, Boolean isRegex, Boolean isFunction, Boolean isValue, String keyName ) {
        if ( !arrayHandles.containsKey( index ) ) {
            initMaps( index, types, isRegex, isFunction, isValue, keyName );
        }
        this.docHandles.get( index ).add( new DocWrapper( key, doc ) );
    }


    public void addKeyHandle( long index, BsonDocument doc, Queue<PolyType> types, Boolean isRegex, Boolean isFunction, Boolean isValue, String keyName ) {
        if ( !arrayHandles.containsKey( index ) ) {
            initMaps( index, types, isRegex, isFunction, isValue, keyName );
        }
        this.keyHandles.get( index ).add(
                new KeyWrapper(
                        0,
                        doc,
                        docHandles.entrySet()
                                .stream()
                                .flatMap( d -> d.getValue().stream() )
                                .filter( w -> w.key.equals( doc.getFirstKey() ) )
                                .collect( Collectors.toList() ) ) );
    }


    /**
     * Stores the information needed to replace a BsonDynamic later on.
     *
     * @param index of the corresponding prepared parameter (?3 -> 3)
     * @param array parent of dynamic
     * @param pos position where object is found from parent ( parent is BsonArray )
     * @param types type of the object itself, to retrieve the correct MongoDB type
     * @param isRegex flag if the BsonDynamic is a regex, which needs to adjusted
     * @param isFunction flag if the BsonDynamic is defined function, which has to be retrieved uniquely
     */
    public void addArrayHandle( long index, BsonArray array, int pos, Queue<PolyType> types, Boolean isRegex, Boolean isFunction, Boolean isValue, String keyName ) {
        if ( !arrayHandles.containsKey( index ) ) {
            initMaps( index, types, isRegex, isFunction, isValue, keyName );
        }
        this.arrayHandles.get( index ).add( new ArrayWrapper( pos, array ) );
    }


    private void initMaps( long index, Queue<PolyType> types, Boolean isRegex, Boolean isFunction, Boolean isValue, String keyName ) {
        this.transformerMap.put( index, BsonUtil.getBsonTransformer( types, bucket ) );
        this.isRegexMap.put( index, isRegex );
        this.isFuncMap.put( index, isFunction );
        this.isValueMap.put( index, isValue );
        this.keyMap.put( index, keyName );
        this.docHandles.put( index, new ArrayList<>() );
        this.arrayHandles.put( index, new ArrayList<>() );
        this.keyHandles.put( index, new ArrayList<>() );
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
                boolean isValue = isValueMap.get( entry.getKey() );
                String key = keyMap.get( entry.getKey() );

                if ( isRegex ) {
                    Consumer<Wrapper> task = el -> el.insert( BsonUtil.replaceLikeWithRegex( (String) entry.getValue() ) );
                    arrayHandles.get( entry.getKey() ).forEach( task );
                    docHandles.get( entry.getKey() ).forEach( task );
                    keyHandles.get( entry.getKey() ).forEach( task );
                } else if ( isFunction ) {
                    // function is always part of a document
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( new BsonString( BsonFunctionHelper.getUsedFunction( entry.getValue() ) ) ) );
                } else if ( isValue ) {
                    Function<Object, BsonValue> transformer = transformerMap.get( entry.getKey() );
                    Consumer<Wrapper> task = el -> el.insert(
                            new BsonString( key
                                    + "."
                                    + transformer.apply( entry.getValue() ).asArray().stream().map( v -> v.asString().getValue() ).collect( Collectors.joining( "." ) ) ) );
                    docHandles.get( entry.getKey() ).forEach( task );
                    arrayHandles.get( entry.getKey() ).forEach( task );
                    keyHandles.get( entry.getKey() ).forEach( task );
                } else {
                    Function<Object, BsonValue> transformer = transformerMap.get( entry.getKey() );
                    Consumer<Wrapper> task;
                    if ( this.isProject ) {
                        task = el -> el.insert( new BsonDocument( "$literal", transformer.apply( entry.getValue() ) ) );
                    } else {
                        task = el -> el.insert( transformer.apply( entry.getValue() ) );
                    }
                    arrayHandles.get( entry.getKey() ).forEach( task );
                    docHandles.get( entry.getKey() ).forEach( task );
                    keyHandles.get( entry.getKey() ).forEach( task );

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


    interface Wrapper {

        void insert( BsonValue value );

    }


    /**
     * Helper class which holds replace information for a BsonDocument, which has one or multiple dynamic children
     * and defines how the child can be replaced.
     */
    static class DocWrapper implements Wrapper {

        @Setter
        private String key;
        final BsonDocument doc;


        DocWrapper( String key, BsonDocument doc ) {
            this.key = key;
            this.doc = doc;
        }


        @Override
        public void insert( BsonValue value ) {
            doc.put( key, value );
        }


    }


    /**
     * Helper class which holds replace information for a BsonDocument, which has one or multiple dynamic children
     * and defines how the child can be replaced.
     */
    static class ArrayWrapper implements Wrapper {

        final int index;
        final BsonArray array;


        ArrayWrapper( int index, BsonArray array ) {
            this.index = index;
            this.array = array;
        }


        @Override
        public void insert( BsonValue value ) {
            array.set( index, value );
        }

    }


    static class KeyWrapper implements Wrapper {

        final int index;
        final BsonDocument document;

        final List<DocWrapper> children;


        KeyWrapper( int index, BsonDocument document, List<DocWrapper> children ) {
            this.index = index;
            this.document = document;
            this.children = children;
        }


        @Override
        public void insert( BsonValue value ) {
            String key = document.getFirstKey();
            BsonValue temp = document.get( key );
            document.remove( key );
            String newKey = value.asString().getValue();
            children.forEach( c -> c.setKey( newKey ) );
            document.put( newKey, temp );
        }

    }

}
