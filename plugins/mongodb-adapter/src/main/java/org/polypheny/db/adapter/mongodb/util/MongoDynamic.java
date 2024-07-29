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
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.functions.PolyValueFunctions;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Pair;


/**
 * Objects of this class represent a blueprint of a dynamic/prepared BsonValue,
 * they initial prepare the correct places to insert later provided dynamic parameters according to the
 * initial provided BsonDocument
 */
@Value
@NonFinal
public class MongoDynamic {

    public static final String MONGO_DYNAMIC_INDEX_KEY = "_dyn";
    public static final String MONGO_DYNAMIC_REGEX_KEY = "_reg";
    public static final String MONGO_DYNAMIC_FUNC_BODY_KEY = "_func";
    public static final String MONGO_DYNAMIC_FUNC_NAME_KEY = "_functionName";
    public static final String MONGO_DYNAMIC_TYPE_KEY = "_type";

    public static final String DOCUMENT_PROJECT_KEY = "$project";

    Map<Long, List<DocWrapper>> docHandles = new HashMap<>(); // parent, key,
    Map<Long, List<ArrayWrapper>> arrayHandles = new HashMap<>(); // parent, index,

    Map<Long, List<KeyWrapper>> keyHandles = new HashMap<>(); // parent, index,
    Map<Long, Function<PolyValue, BsonValue>> transformers = new HashMap<>();
    GridFSBucket bucket;
    BsonDocument document;
    Map<Long, Boolean> isRegexMap = new HashMap<>();
    Map<Long, Boolean> isFuncMap = new HashMap<>();
    boolean isProject;

    Map<Long, String> keyMap = new HashMap<>();
    Map<Long, String> functionNames = new HashMap<>();


    public MongoDynamic( BsonDocument document, GridFSBucket bucket ) {
        this.document = document.clone();
        this.bucket = bucket;
        this.isProject = !document.isEmpty() && document.getFirstKey().equals( DOCUMENT_PROJECT_KEY );
        this.document.forEach( ( k, bsonValue ) -> replaceDynamic( bsonValue, this.document, k, true, false ) );
    }


    public static MongoDynamic create( BsonDocument document, GridFSBucket bucket, DataModel dataModel ) {
        if ( dataModel == DataModel.DOCUMENT ) {
            return new MongoDocumentDynamic( document, bucket );
        }
        return new MongoDynamic( document, bucket );
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
            if ( ((BsonDocument) preDocument).containsKey( MONGO_DYNAMIC_INDEX_KEY ) ) {
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
        BsonValue bsonIndex = preDocument.get( MONGO_DYNAMIC_INDEX_KEY );
        Boolean isRegex = preDocument.get( MONGO_DYNAMIC_REGEX_KEY ).asBoolean().getValue();
        Boolean isFunction = preDocument.get( MONGO_DYNAMIC_FUNC_BODY_KEY ).asBoolean().getValue();
        String functionName = preDocument.get( MONGO_DYNAMIC_FUNC_NAME_KEY ).isNull() ? null : preDocument.get( MONGO_DYNAMIC_FUNC_NAME_KEY ).asString().getValue();
        long pos;
        if ( bsonIndex.isInt64() ) {
            pos = bsonIndex.asInt64().getValue();
        } else {
            pos = bsonIndex.asInt32().getValue();
        }
        Queue<Pair<PolyType, Optional<Integer>>> polyTypes = Arrays.stream( preDocument.get( MONGO_DYNAMIC_TYPE_KEY ).asString().getValue().split( "\\$" ) ).map( type -> type.split( "\\|" ) ).map( p -> Pair.of( PolyType.valueOf( p[0] ), (p.length > 1 ? Optional.of( Integer.parseInt( p[1] ) ) : Optional.ofNullable( (Integer) null )) ) ).collect( Collectors.toCollection( LinkedList::new ) );

        if ( isDoc ) {
            addDocHandle( pos, (BsonDocument) parent, (String) key, polyTypes, isRegex, isFunction, functionName );
        } else if ( isKey ) {
            addKeyHandle( pos, (BsonDocument) parent, polyTypes, isRegex, isFunction, functionName );
        } else {
            addArrayHandle( pos, (BsonArray) parent, (int) key, polyTypes, isRegex, isFunction, functionName );
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
     * @param functionName name of the function
     */
    public void addDocHandle( long index, BsonDocument doc, String key, Queue<Pair<PolyType, Optional<Integer>>> types, Boolean isRegex, Boolean isFunction, String functionName ) {
        if ( !arrayHandles.containsKey( index ) ) {
            initMaps( index, types, isRegex, isFunction, functionName );
        }
        this.docHandles.get( index ).add( new DocWrapper( key, doc ) );
    }


    public void addKeyHandle( long index, BsonDocument doc, Queue<Pair<PolyType, Optional<Integer>>> types, Boolean isRegex, Boolean isFunction, String functionName ) {
        if ( !arrayHandles.containsKey( index ) ) {
            initMaps( index, types, isRegex, isFunction, functionName );
        }
        this.keyHandles.get( index ).add(
                new KeyWrapper(
                        0,
                        doc,
                        docHandles.entrySet()
                                .stream()
                                .flatMap( d -> d.getValue().stream() )
                                .filter( w -> w.key.equals( doc.getFirstKey() ) )
                                .toList() ) );
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
     * @param functionName name of the function
     */
    public void addArrayHandle( long index, BsonArray array, int pos, Queue<Pair<PolyType, Optional<Integer>>> types, Boolean isRegex, Boolean isFunction, String functionName ) {
        if ( !arrayHandles.containsKey( index ) ) {
            initMaps( index, types, isRegex, isFunction, functionName );
        }
        this.arrayHandles.get( index ).add( new ArrayWrapper( pos, array ) );
    }


    private void initMaps( long index, Queue<Pair<PolyType, Optional<Integer>>> types, Boolean isRegex, Boolean isFunction, String functionName ) {
        this.transformers.put( index, BsonUtil.getBsonTransformer( types, bucket ) );
        this.isRegexMap.put( index, isRegex );
        this.isFuncMap.put( index, isFunction );
        this.docHandles.put( index, new ArrayList<>() );
        this.arrayHandles.put( index, new ArrayList<>() );
        this.keyHandles.put( index, new ArrayList<>() );
        if ( functionName != null ) {
            this.functionNames.put( index, functionName );
        }
    }


    /**
     * Insert operation, which replaces the initially defined dynamic/prepared objects the provided values.
     *
     * @param parameterValues the dynamic parameters
     * @return a final BsonObject with the correct values inserted
     */
    public BsonDocument insert( Map<Long, PolyValue> parameterValues ) {
        for ( Entry<Long, PolyValue> entry : parameterValues.entrySet() ) {
            if ( arrayHandles.containsKey( entry.getKey() ) ) {
                Boolean isRegex = isRegexMap.get( entry.getKey() );
                Boolean isFunction = isFuncMap.get( entry.getKey() );
                String functionName = functionNames.get( entry.getKey() );

                if ( isRegex ) {
                    Consumer<Wrapper> task = el -> el.insert( BsonUtil.replaceLikeWithRegex( entry.getValue().asString().value ) );
                    arrayHandles.get( entry.getKey() ).forEach( task );
                    docHandles.get( entry.getKey() ).forEach( task );
                    keyHandles.get( entry.getKey() ).forEach( task );
                } else if ( isFunction ) {
                    // function is always part of a document
                    docHandles.get( entry.getKey() ).forEach( el -> el.insert( new BsonString( BsonFunctionHelper.getUsedFunction( entry.getValue() ) ) ) );
                } else {
                    Function<PolyValue, BsonValue> transformer = transformers.get( entry.getKey() );
                    Consumer<Wrapper> task;

                    Function<PolyValue, PolyValue> wrapper = functionName != null ? PolyValueFunctions.pickWrapper( functionName ) : e -> e;

                    if ( this.isProject ) {
                        task = el -> el.insert( new BsonDocument( "$literal", transformer.apply( wrapper.apply( entry.getValue() ) ) ) );
                    } else {
                        task = el -> el.insert( transformer.apply( wrapper.apply( entry.getValue() ) ) );
                    }
                    arrayHandles.get( entry.getKey() ).forEach( task );
                    docHandles.get( entry.getKey() ).forEach( task );
                    keyHandles.get( entry.getKey() ).forEach( task );

                }
            }
        }
        return unwrapDynamicKeys( document.clone() ).asDocument();
    }


    private BsonValue unwrapDynamicKeys( BsonValue value ) {
        if ( value.getBsonType() == BsonType.DOCUMENT ) {
            BsonDocument doc = value.asDocument();
            BsonDocument newDoc = new BsonDocument();
            for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
                if ( entry.getKey().startsWith( "_kv_" ) ) {
                    if ( entry.getValue().isDocument() ) {
                        newDoc.put( entry.getValue().asDocument().get( "_k_" ).asString().getValue(), entry.getValue().asDocument().get( "_v_" ) );
                    } else {
                        newDoc.put( entry.getValue().asArray().get( 0 ).asDocument().get( "_k_" ).asString().getValue(), entry.getValue().asArray().get( 0 ).asDocument().get( "_v_" ) );
                    }

                } else {
                    newDoc.put( entry.getKey(), unwrapDynamicKeys( entry.getValue() ) );
                }
            }
            return newDoc;
        } else if ( value.getBsonType() == BsonType.ARRAY ) {
            BsonArray array = value.asArray();
            for ( int i = 0; i < array.size(); i++ ) {
                array.set( i, unwrapDynamicKeys( array.get( i ) ) );
            }
            return array;
        }
        return value;
    }


    /**
     * Fully prepare a batch of multiple prepared rows and transform them into BsonDocuments.
     *
     * @param parameterValues multiple rows of dynamic parameters
     * @param constructor the initial constructor, which holds the blueprint to replace the dynamic parameter
     * @return a list of rows, which can directly be inserted
     */
    public List<? extends WriteModel<Document>> getAll(
            List<Map<Long, PolyValue>> parameterValues,
            Function<Document, ? extends WriteModel<Document>> constructor ) {
        return parameterValues.stream()
                .map( value -> constructor.apply( BsonUtil.asDocument( insert( value ) ) ) )
                .toList();
    }


    public List<Document> getAll( List<Map<Long, PolyValue>> parameterValues ) {
        return parameterValues.stream().map( value -> BsonUtil.asDocument( insert( value ) ) ).toList();
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
    record ArrayWrapper( int index, BsonArray array ) implements Wrapper {


        @Override
        public void insert( BsonValue value ) {
            array.set( index, value );
        }

    }


    record KeyWrapper( int index, BsonDocument document, List<DocWrapper> children ) implements Wrapper {


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


    public static class MongoDocumentDynamic extends MongoDynamic {

        public MongoDocumentDynamic( BsonDocument document, GridFSBucket bucket ) {
            super( document, bucket );
        }


        @Override
        public List<Document> getAll( List<Map<Long, PolyValue>> parameterValues ) {
            return parameterValues.stream().flatMap( e -> e.values().stream().map( polyValue -> Document.parse( polyValue.asDocument().toJson() ) ) ).collect( Collectors.toList() );
        }


    }

}
