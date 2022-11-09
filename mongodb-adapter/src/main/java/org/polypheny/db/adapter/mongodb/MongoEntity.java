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


import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.mongodb.util.MongoDynamic;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableCollection;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Util;


/**
 * Table based on a MongoDB collection.
 */
@Slf4j
public class MongoEntity extends AbstractQueryableTable implements TranslatableTable, ModifiableTable, ModifiableCollection {

    @Getter
    private final String collectionName;
    @Getter
    private final AlgProtoDataType protoRowType;
    @Getter
    private final MongoSchema mongoSchema;
    @Getter
    private final MongoCollection<Document> collection;
    @Getter
    private final CatalogTable catalogTable;

    @Getter
    private final CatalogCollection catalogCollection;
    @Getter
    private final TransactionProvider transactionProvider;
    @Getter
    private final int storeId;


    /**
     * Creates a MongoTable.
     */
    MongoEntity( CatalogTable catalogTable, MongoSchema schema, AlgProtoDataType proto, TransactionProvider transactionProvider, int storeId, CatalogPartitionPlacement partitionPlacement ) {
        super( Object[].class );
        this.collectionName = MongoStore.getPhysicalTableName( catalogTable.id, partitionPlacement.partitionId );
        this.transactionProvider = transactionProvider;
        this.catalogTable = catalogTable;
        this.catalogCollection = null;
        this.protoRowType = proto;
        this.mongoSchema = schema;
        this.collection = schema.database.getCollection( collectionName );
        this.storeId = storeId;
        this.tableId = catalogTable.id;
    }


    public MongoEntity( CatalogCollection catalogEntity, MongoSchema schema, AlgProtoDataType proto, TransactionProvider transactionProvider, int adapter, CatalogCollectionPlacement partitionPlacement ) {
        super( Object[].class );
        this.collectionName = MongoStore.getPhysicalTableName( catalogEntity.id, partitionPlacement.id );
        this.transactionProvider = transactionProvider;
        this.catalogTable = null;
        this.catalogCollection = catalogEntity;
        this.protoRowType = proto;
        this.mongoSchema = schema;
        this.collection = schema.database.getCollection( collectionName );
        this.storeId = adapter;
        this.tableId = catalogEntity.id;
    }


    public String toString() {
        return "MongoTable {" + collectionName + "}";
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new MongoQueryable<>( dataContext, schema, this, tableName );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        final AlgOptCluster cluster = context.getCluster();
        return new MongoScan( cluster, cluster.traitSetOf( MongoAlg.CONVENTION ), algOptTable, this, null );
    }


    /**
     * Executes a "find" operation on the underlying collection.
     *
     * For example,
     * <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code>
     *
     * @param mongoDb MongoDB connection
     * @param filterJson Filter JSON string, or null
     * @param projectJson Project JSON string, or null
     * @param fields List of fields to project; or null to return map
     * @return Enumerator of results
     */
    private Enumerable<Object> find( MongoDatabase mongoDb, MongoEntity table, String filterJson, String projectJson, List<Entry<String, Class>> fields, List<Entry<String, Class>> arrayFields ) {
        final MongoCollection<Document> collection = mongoDb.getCollection( collectionName );
        final Bson filter = filterJson == null ? new BsonDocument() : BsonDocument.parse( filterJson );
        final Bson project = projectJson == null ? new BsonDocument() : BsonDocument.parse( projectJson );
        final Function1<Document, Object> getter = MongoEnumerator.getter( fields, arrayFields );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                final FindIterable<Document> cursor = collection.find( filter ).projection( project );
                return new MongoEnumerator( cursor.iterator(), getter, table.getMongoSchema().getBucket() );
            }
        };
    }


    /**
     * Executes an "aggregate" operation on the underlying collection.
     *
     * For example:
     * <code>zipsTable.aggregate(
     * "{$filter: {state: 'OR'}",
     * "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: '$pop'}}}")
     * </code>
     *
     * @param dataContext the context, which specifies multiple parameters regarding data
     * @param session the sessions to which the aggregation belongs
     * @param mongoDb MongoDB connection
     * @param fields List of fields to project; or null to return map
     * @param operations One or more JSON strings
     * @param parameterValues the values pre-ordered
     * @return Enumerator of results
     */
    private Enumerable<Object> aggregate(
            DataContext dataContext, ClientSession session,
            final MongoDatabase mongoDb,
            MongoEntity table,
            final List<Entry<String, Class>> fields,
            List<Entry<String, Class>> arrayFields,
            final List<String> operations,
            Map<Long, Object> parameterValues,
            //BsonDocument filter,
            List<BsonDocument> preOps,
            List<String> logicalCols ) {
        final List<BsonDocument> list = new ArrayList<>();

        if ( parameterValues.size() == 0 ) {
            // direct query
            preOps.forEach( op -> list.add( new BsonDocument( "$addFields", op ) ) );

            for ( String operation : operations ) {
                list.add( BsonDocument.parse( operation ) );
            }
        } else {
            // prepared
            preOps.stream()
                    .map( op -> new MongoDynamic( new BsonDocument( "$addFields", op ), mongoSchema.getBucket(), dataContext ) )
                    .forEach( util -> list.add( util.insert( parameterValues ) ) );

            for ( String operation : operations ) {
                MongoDynamic opUtil = new MongoDynamic( BsonDocument.parse( operation ), getMongoSchema().getBucket(), dataContext );
                list.add( opUtil.insert( parameterValues ) );
            }
        }

        if ( logicalCols.size() != 0 && catalogTable != null ) {
            list.add( 0, getPhysicalProjections( logicalCols, catalogTable.getColumnNames(), catalogTable.fieldIds ) );
        }

        final Function1<Document, Object> getter = MongoEnumerator.getter( fields, arrayFields );

        if ( log.isDebugEnabled() ) {
            log.debug( list.stream().map( el -> el.toBsonDocument().toJson( JsonWriterSettings.builder().outputMode( JsonMode.SHELL ).build() ) ).collect( Collectors.joining( ",\n" ) ) );
        }

        // empty docs are possible
        if ( list.isEmpty() ) {
            list.add( new BsonDocument( "$match", new BsonDocument() ) );
        }
        //list.forEach( el -> System.out.println( el.toBsonDocument().toJson( JsonWriterSettings.builder().outputMode( JsonMode.SHELL ).build() ) ) );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                final Iterator<Document> resultIterator;
                try {
                    if ( list.size() != 0 ) {
                        resultIterator = mongoDb.getCollection( collectionName ).aggregate( session, list ).iterator();
                    } else {
                        resultIterator = Collections.emptyIterator();
                    }
                } catch ( Exception e ) {
                    throw new RuntimeException( "While running MongoDB query " + Util.toString( list, "[", ",\n", "]" ), e );
                }
                return new MongoEnumerator( resultIterator, getter, table.getMongoSchema().getBucket() );
            }
        };
    }


    public static BsonDocument getPhysicalProjections( List<String> logicalCols, List<String> fieldNames, List<Long> fieldIds ) {
        BsonDocument projections = new BsonDocument();
        for ( String logicalCol : logicalCols ) {
            int index = fieldNames.indexOf( logicalCol );
            if ( index != -1 ) {
                projections.append( logicalCol, new BsonString( "$" + MongoStore.getPhysicalColumnName( logicalCol, fieldIds.get( index ) ) ) );
            } else {
                projections.append( logicalCol, new BsonInt32( 1 ) );
            }
        }
        return new BsonDocument( "$project", projections );
    }


    /**
     * Helper method to strip non-numerics from a string.
     *
     * Currently used to determine mongod versioning numbers
     * from buildInfo.versionArray for use in aggregate method logic.
     */
    private static Integer parseIntString( String valueString ) {
        return Integer.parseInt( valueString.replaceAll( "[^0-9]", "" ) );
    }


    @Override
    public Collection getModifiableCollection() {
        throw new RuntimeException( "getModifiableCollection() is not implemented for MongoDB adapter!" );
    }


    @Override
    public Modify toModificationAlg(
            AlgOptCluster cluster,
            AlgOptTable table,
            CatalogReader catalogReader,
            AlgNode child,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        mongoSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
    }


    @Override
    public DocumentModify toModificationAlg(
            AlgOptCluster cluster,
            AlgOptTable table,
            CatalogReader catalogReader,
            AlgNode child,
            Operation operation,
            List<String> keys,
            List<RexNode> updates ) {
        mongoSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalDocumentModify( child.getTraitSet(), table, catalogReader, child, operation, keys, updates );
    }


    /**
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a {@link MongoEntity}.
     *
     * @param <T> element type
     */
    public static class MongoQueryable<T> extends AbstractTableQueryable<T> {

        MongoQueryable( DataContext dataContext, SchemaPlus schema, MongoEntity table, String tableName ) {
            super( dataContext, schema, table, tableName );
        }


        @Override
        public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable = (Enumerable<T>) getTable().find( getMongoDb(), getTable(), null, null, null, null );
            return enumerable.enumerator();
        }


        private MongoDatabase getMongoDb() {
            return schema.unwrap( MongoSchema.class ).database;
        }


        private MongoEntity getTable() {
            return (MongoEntity) table;
        }


        /**
         * Called via code-generation.
         *
         * @see MongoMethod#MONGO_QUERYABLE_AGGREGATE
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> aggregate( List<Map.Entry<String, Class>> fields, List<Map.Entry<String, Class>> arrayClass, List<String> operations, List<String> preProjections, List<String> logicalCols ) {
            ClientSession session = getTable().getTransactionProvider().getSession( dataContext.getStatement().getTransaction().getXid() );
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( this.getTable().getStoreId() ) );

            Map<Long, Object> values = new HashMap<>();
            if ( dataContext.getParameterValues().size() == 1 ) {
                values = dataContext.getParameterValues().get( 0 );
            }

            return getTable().aggregate(
                    dataContext,
                    session,
                    getMongoDb(),
                    getTable(),
                    fields,
                    arrayClass,
                    operations,
                    values,
                    preProjections.stream().map( BsonDocument::parse ).collect( Collectors.toList() ),
                    logicalCols );
        }


        /**
         * Called via code-generation.
         *
         * @param filterJson Filter document
         * @param projectJson Projection document
         * @param fields List of expected fields (and their types)
         * @return result of mongo query
         * @see MongoMethod#MONGO_QUERYABLE_FIND
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> find( String filterJson, String projectJson, List<Map.Entry<String, Class>> fields, List<Map.Entry<String, Class>> arrayClasses ) {
            return getTable().find( getMongoDb(), getTable(), filterJson, projectJson, fields, arrayClasses );
        }


        /**
         * This method handles direct DMLs(which already have the values)
         *
         * @param operation which defines which kind the DML is
         * @param filter filter operations
         * @param operations the collection of values to insert
         * @return the enumerable which holds the result of the operation
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> handleDirectDML( Operation operation, String filter, List<String> operations, boolean onlyOne, boolean needsDocument ) {
            MongoEntity mongoEntity = getTable();
            PolyXid xid = dataContext.getStatement().getTransaction().getXid();
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( mongoEntity.getStoreId() ) );
            GridFSBucket bucket = mongoEntity.getMongoSchema().getBucket();

            try {
                final long changes = doDML( operation, filter, operations, onlyOne, needsDocument, mongoEntity, xid, bucket );

                return Linq4j.asEnumerable( Collections.singletonList( changes ) );
            } catch ( MongoException e ) {
                mongoEntity.getTransactionProvider().rollback( xid );
                log.warn( "Failed" );
                log.warn( String.format( "op: %s\nfilter: %s\nops: [%s]", operation.name(), filter, String.join( ";", operations ) ) );
                log.warn( e.getMessage() );
                throw new RuntimeException( e.getMessage(), e );
            }
        }


        private long doDML( Operation operation, String filter, List<String> operations, boolean onlyOne, boolean needsDocument, MongoEntity mongoEntity, PolyXid xid, GridFSBucket bucket ) {
            ClientSession session = mongoEntity.getTransactionProvider().startTransaction( xid, true );

            long changes = 0;
            switch ( operation ) {
                case INSERT:
                    if ( dataContext.getParameterValues().size() != 0 ) {
                        assert operations.size() == 1;
                        // prepared
                        MongoDynamic util = new MongoDynamic( BsonDocument.parse( operations.get( 0 ) ), bucket, dataContext );
                        List<Document> inserts = util.getAll( dataContext.getParameterValues() );
                        mongoEntity.getCollection().insertMany( session, inserts );
                        return inserts.size();
                    } else {
                        // direct
                        List<Document> docs = operations.stream().map( BsonDocument::parse ).map( BsonUtil::asDocument ).collect( Collectors.toList() );
                        mongoEntity.getCollection().insertMany( session, docs );
                        return docs.size();
                    }

                case UPDATE:
                    assert operations.size() == 1;
                    // we use only update docs
                    if ( dataContext.getParameterValues().size() != 0 ) {
                        // prepared we use document update not pipeline
                        MongoDynamic filterUtil = new MongoDynamic( BsonDocument.parse( filter ), bucket, dataContext );
                        MongoDynamic docUtil = new MongoDynamic( BsonDocument.parse( operations.get( 0 ) ), bucket, dataContext );
                        for ( Map<Long, Object> parameterValue : dataContext.getParameterValues() ) {
                            if ( onlyOne ) {
                                if ( needsDocument ) {
                                    changes += mongoEntity
                                            .getCollection()
                                            .updateOne( session, filterUtil.insert( parameterValue ), docUtil.insert( parameterValue ) )
                                            .getModifiedCount();
                                } else {
                                    changes += mongoEntity
                                            .getCollection()
                                            .updateOne( session, filterUtil.insert( parameterValue ), Collections.singletonList( docUtil.insert( parameterValue ) ) )
                                            .getModifiedCount();
                                }
                            } else {
                                if ( needsDocument ) {
                                    changes += mongoEntity
                                            .getCollection()
                                            .updateMany( session, filterUtil.insert( parameterValue ), docUtil.insert( parameterValue ) )
                                            .getModifiedCount();
                                } else {
                                    changes += mongoEntity
                                            .getCollection()
                                            .updateMany( session, filterUtil.insert( parameterValue ), Collections.singletonList( docUtil.insert( parameterValue ) ) )
                                            .getModifiedCount();
                                }
                            }
                        }
                    } else {
                        // direct
                        if ( onlyOne ) {
                            changes = mongoEntity
                                    .getCollection()
                                    .updateOne( session, BsonDocument.parse( filter ), BsonDocument.parse( operations.get( 0 ) ) )
                                    .getModifiedCount();
                        } else {
                            changes = mongoEntity
                                    .getCollection()
                                    .updateMany( session, BsonDocument.parse( filter ), BsonDocument.parse( operations.get( 0 ) ) )
                                    .getModifiedCount();
                        }

                    }
                    break;

                case DELETE:
                    if ( dataContext.getParameterValues().size() != 0 ) {
                        // prepared
                        MongoDynamic filterUtil = new MongoDynamic( BsonDocument.parse( filter ), bucket, dataContext );
                        List<? extends WriteModel<Document>> filters;
                        if ( onlyOne ) {
                            filters = filterUtil.getAll( dataContext.getParameterValues(), DeleteOneModel::new );
                        } else {
                            filters = filterUtil.getAll( dataContext.getParameterValues(), DeleteManyModel::new );
                        }

                        changes = mongoEntity.getCollection().bulkWrite( session, filters ).getDeletedCount();
                    } else {
                        // direct
                        if ( onlyOne ) {
                            changes = mongoEntity
                                    .getCollection()
                                    .deleteOne( session, BsonDocument.parse( filter ) )
                                    .getDeletedCount();
                        } else {
                            changes = mongoEntity
                                    .getCollection()
                                    .deleteMany( session, BsonDocument.parse( filter ) )
                                    .getDeletedCount();
                        }
                    }
                    break;

                case MERGE:
                    throw new RuntimeException( "MERGE IS NOT SUPPORTED" );
            }
            return changes;

        }

    }

}
