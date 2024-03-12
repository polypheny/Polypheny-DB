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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.mongodb.rules.MongoScan;
import org.polypheny.db.adapter.mongodb.util.MongoDynamic;
import org.polypheny.db.adapter.mongodb.util.MongoTupleType;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.impl.AbstractEntityQueryable;
import org.polypheny.db.schema.types.ModifiableCollection;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Util;


/**
 * Table based on a MongoDB collection.
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Value
@SuperBuilder(toBuilder = true)
public class MongoEntity extends PhysicalEntity implements TranslatableEntity, ModifiableTable, ModifiableCollection, QueryableEntity {

    @Getter
    public MongoNamespace mongoNamespace;
    @Getter
    public TransactionProvider transactionProvider;
    @Getter
    public long storeId;
    public PhysicalEntity physical;

    @Getter
    public MongoCollection<Document> collection;
    public List<? extends PhysicalField> fields;


    /**
     * Creates a MongoTable.
     */
    MongoEntity( PhysicalEntity physical, List<? extends PhysicalField> fields, MongoNamespace namespace, TransactionProvider transactionProvider ) {
        super( physical.id, physical.allocationId, physical.logicalId, physical.name, physical.namespaceId, physical.namespaceName.toLowerCase(), physical.getUniqueFieldIds(), physical.dataModel, physical.adapterId );
        this.physical = physical;
        this.mongoNamespace = namespace;
        this.transactionProvider = transactionProvider;
        this.storeId = physical.adapterId;
        this.collection = namespace.database.getCollection( physical.name );
        this.fields = fields;
    }


    @Override
    public AlgDataType getTupleType() {
        if ( dataModel == DataModel.RELATIONAL ) {
            return buildProto().apply( AlgDataTypeFactory.DEFAULT );
        }
        return super.getTupleType();
    }


    public AlgProtoDataType buildProto() {
        final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

        for ( PhysicalColumn column : fields.stream().map( f -> f.unwrap( PhysicalColumn.class ).orElseThrow() ).sorted( Comparator.comparingInt( a -> a.position ) ).toList() ) {
            AlgDataType sqlType = column.getAlgDataType( AlgDataTypeFactory.DEFAULT );
            fieldInfo.add( column.id, column.logicalName, column.name, sqlType ).nullable( column.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    public String toString() {
        return "MongoEntity {" + physical.name + "}";
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new MongoScan( cluster, traitSet.replace( MongoAlg.CONVENTION ), this );
    }


    /**
     * Executes a "find" operation on the underlying collection.
     * <p>
     * For example,
     * <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code>
     *
     * @param mongoDb MongoDB connection
     * @param filterJson Filter JSON string, or null
     * @param projectJson Project JSON string, or null
     * @param tupleType List of fields to project; or null to return map
     * @return Enumerator of results
     */
    private Enumerable<PolyValue[]> find( MongoDatabase mongoDb, MongoEntity table, String filterJson, String projectJson, MongoTupleType tupleType ) {
        final Bson filter = filterJson == null ? new BsonDocument() : BsonDocument.parse( filterJson );
        final Bson project = projectJson == null ? new BsonDocument() : BsonDocument.parse( projectJson );
        final Function1<Document, PolyValue[]> getter = MongoEnumerator.getter( tupleType );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                final FindIterable<Document> cursor = collection.find( filter ).projection( project );
                return new MongoEnumerator( cursor.map( getter::apply ).iterator(), table.getMongoNamespace().getBucket() );
            }
        };
    }


    /**
     * Executes an "aggregate" operation on the underlying collection.
     * <p>
     * For example:
     * <code>zipsTable.aggregate(
     * "{$filter: {state: 'OR'}",
     * "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: '$pop'}}}")
     * </code>
     *
     * @param dataContext the context, which specifies multiple parameters regarding data
     * @param session the sessions to which the aggregation belongs
     * @param mongoDb MongoDB connection
     * @param tupleType List of fields to project; or null to return map
     * @param operations One or more JSON strings
     * @param parameterValues the values pre-ordered
     * @return Enumerator of results
     */
    public Enumerable<PolyValue[]> aggregate(
            DataContext dataContext,
            ClientSession session,
            final MongoDatabase mongoDb,
            MongoEntity table,
            MongoTupleType tupleType,
            final List<String> operations,
            Map<Long, PolyValue> parameterValues,
            List<BsonDocument> preOps ) {
        final List<BsonDocument> list = new ArrayList<>();

        if ( parameterValues.isEmpty() ) {
            // direct query
            preOps.forEach( op -> list.add( new BsonDocument( "$addFields", op ) ) );

            for ( String operation : operations ) {
                list.add( BsonDocument.parse( operation ) );
            }
        } else {
            // prepared
            preOps.stream()
                    .map( op -> new MongoDynamic( new BsonDocument( "$addFields", op ), mongoNamespace.getBucket() ) )
                    .forEach( util -> list.add( util.insert( parameterValues ) ) );

            for ( String operation : operations ) {
                MongoDynamic opUtil = new MongoDynamic( BsonDocument.parse( operation ), getMongoNamespace().getBucket() );
                list.add( opUtil.insert( parameterValues ) );
            }
        }

        final Function1<Document, PolyValue[]> getter = MongoEnumerator.getter( tupleType );

        if ( log.isDebugEnabled() ) {
            log.warn( list.stream().map( el -> el.toBsonDocument().toJson( JsonWriterSettings.builder().outputMode( JsonMode.SHELL ).build() ) ).collect( Collectors.joining( ",\n" ) ) );
        }

        // empty docs are possible
        if ( list.isEmpty() ) {
            list.add( new BsonDocument( "$match", new BsonDocument() ) );
        }

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                final Iterator<PolyValue[]> resultIterator;
                try {
                    if ( !list.isEmpty() ) {
                        resultIterator = mongoDb.getCollection( physical.name ).aggregate( session, list ).map( getter::apply ).iterator();
                    } else {
                        resultIterator = Collections.emptyIterator();
                    }
                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "While running MongoDB query " + Util.toString( list, "[", ",\n", "]" ), e );
                }
                return new MongoEnumerator( resultIterator, table.getMongoNamespace().getBucket() );
            }
        };
    }


    @Override
    public Modify<?> toModificationTable(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            Entity table,
            AlgNode child,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList ) {
        mongoNamespace.getConvention().register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster.traitSetOf( Convention.NONE ),
                table,
                child,
                operation,
                updateColumnList,
                sourceExpressionList );
    }


    @Override
    public Modify<?> toModificationCollection(
            AlgCluster cluster,
            AlgTraitSet traits,
            Entity collection,
            AlgNode child,
            Operation operation,
            Map<String, ? extends RexNode> updates,
            Map<String, String> renames,
            List<String> removes ) {
        mongoNamespace.getConvention().register( cluster.getPlanner() );
        return new LogicalDocumentModify(
                cluster.traitSetOf( Convention.NONE ),
                collection,
                child,
                operation,
                updates,
                removes,
                renames );
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call(
                Expressions.convert_(
                        Expressions.call(
                                Expressions.call(
                                        mongoNamespace.getStore().getCatalogAsExpression(),
                                        "getPhysical", Expressions.constant( id ) ),
                                "unwrapOrThrow", Expressions.constant( MongoEntity.class ) ),
                        MongoEntity.class ),
                "asQueryable",
                DataContext.ROOT,
                Catalog.SNAPSHOT_EXPRESSION );
    }


    @Override
    public MongoQueryable<PolyValue[]> asQueryable( DataContext dataContext, Snapshot snapshot ) {
        return new MongoQueryable<>( dataContext, snapshot, this );
    }


    @Override
    public PhysicalEntity normalize() {
        return new PhysicalCollection( id, allocationId, logicalId, namespaceId, name, namespaceName, adapterId );
    }


    public String getPhysicalName( String logicalName ) {
        return fields.stream().filter( f -> f.logicalName.equals( logicalName ) ).map( f -> f.name ).findFirst().orElse( null );
    }


    /**
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a {@link MongoEntity}.
     *
     * @param <T> element type
     */
    public static class MongoQueryable<T> extends AbstractEntityQueryable<T, MongoEntity> {

        MongoQueryable( DataContext dataContext, Snapshot snapshot, MongoEntity entity ) {
            super( dataContext, snapshot, entity );
        }


        @Override
        public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable = (Enumerable<T>) getEntity().find( getMongoDb(), getEntity(), null, null, null );
            return enumerable.enumerator();
        }


        private MongoDatabase getMongoDb() {
            return entity.mongoNamespace.database;
        }


        private MongoEntity getEntity() {
            return entity;
        }


        /**
         * Called via code-generation.
         *
         * @see MongoMethod#MONGO_QUERYABLE_AGGREGATE
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> aggregate( MongoTupleType tupleType, List<String> operations, List<String> preProjections, List<String> logicalCols ) {
            ClientSession session = getEntity().getTransactionProvider().getSession( dataContext.getStatement().getTransaction().getXid() );
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( (int) this.getEntity().getStoreId() ).orElseThrow() );

            Map<Long, PolyValue> values = new HashMap<>();
            if ( dataContext.getParameterValues().size() == 1 ) {
                values = dataContext.getParameterValues().get( 0 );
            }

            return getEntity().aggregate(
                    dataContext,
                    session,
                    getMongoDb(),
                    getEntity(),
                    tupleType,
                    operations,
                    values,
                    preProjections.stream().map( BsonDocument::parse ).toList() );
        }


        /**
         * Called via code-generation.
         *
         * @param filterJson Filter document
         * @param projectJson Projection document
         * @param tupleType List of expected fields (and their types)
         * @return result of mongo query
         * @see MongoMethod#MONGO_QUERYABLE_FIND
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> find( String filterJson, String projectJson, MongoTupleType tupleType ) {
            return getEntity().find( getMongoDb(), getEntity(), filterJson, projectJson, tupleType );
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
            PolyXid xid = dataContext.getStatement().getTransaction().getXid();
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( entity.getAdapterId() ).orElseThrow() );
            GridFSBucket bucket = entity.getMongoNamespace().getBucket();

            try {
                final long changes = doDML( operation, filter, operations, onlyOne, needsDocument, xid, bucket );

                return Linq4j.asEnumerable( Collections.singletonList( new PolyValue[]{ PolyLong.of( changes ) } ) );
            } catch ( MongoException e ) {
                entity.getTransactionProvider().rollback( xid );
                log.warn( "Failed" );
                log.warn( String.format( "op: %s\nfilter: %s\nops: [%s]", operation.name(), filter, String.join( ";", operations ) ) );
                log.warn( e.getMessage() );
                throw new GenericRuntimeException( e.getMessage(), e );
            }
        }


        private long doDML( Operation operation, String filter, List<String> operations, boolean onlyOne, boolean needsDocument, PolyXid xid, GridFSBucket bucket ) {
            ClientSession session = entity.getTransactionProvider().startTransaction( xid, true );

            long changes = 0;
            switch ( operation ) {
                case INSERT:
                    if ( !dataContext.getParameterValues().isEmpty() ) {
                        assert operations.size() == 1;
                        // prepared
                        MongoDynamic util = MongoDynamic.create( BsonDocument.parse( operations.get( 0 ) ), bucket, getEntity().getDataModel() );
                        List<Document> inserts = util.getAll( dataContext.getParameterValues() );
                        entity.getCollection().insertMany( session, inserts );
                        return inserts.size();
                    } else {
                        // direct
                        List<Document> docs = operations.stream().map( BsonDocument::parse ).map( BsonUtil::asDocument ).toList();
                        entity.getCollection().insertMany( session, docs );
                        return docs.size();
                    }

                case UPDATE:
                    assert operations.size() == 1;
                    // we use only update docs
                    if ( !dataContext.getParameterValues().isEmpty() ) {
                        // prepared we use document update not pipeline
                        MongoDynamic filterUtil = new MongoDynamic( BsonDocument.parse( filter ), bucket );
                        MongoDynamic docUtil = new MongoDynamic( BsonDocument.parse( operations.get( 0 ) ), bucket );
                        for ( Map<Long, PolyValue> parameterValue : dataContext.getParameterValues() ) {
                            if ( onlyOne ) {
                                if ( needsDocument ) {
                                    changes += entity
                                            .getCollection()
                                            .updateOne( session, filterUtil.insert( parameterValue ), docUtil.insert( parameterValue ) )
                                            .getModifiedCount();
                                } else {
                                    changes += entity
                                            .getCollection()
                                            .updateOne( session, filterUtil.insert( parameterValue ), Collections.singletonList( docUtil.insert( parameterValue ) ) )
                                            .getModifiedCount();
                                }
                            } else {
                                if ( needsDocument ) {
                                    changes += entity
                                            .getCollection()
                                            .updateMany( session, filterUtil.insert( parameterValue ), docUtil.insert( parameterValue ) )
                                            .getModifiedCount();
                                } else {
                                    changes += entity
                                            .getCollection()
                                            .updateMany( session, filterUtil.insert( parameterValue ), Collections.singletonList( docUtil.insert( parameterValue ) ) )
                                            .getModifiedCount();
                                }
                            }
                        }
                    } else {
                        // direct
                        if ( onlyOne ) {
                            changes = entity
                                    .getCollection()
                                    .updateOne( session, BsonDocument.parse( filter ), List.of( BsonDocument.parse( operations.get( 0 ) ) ) )
                                    .getModifiedCount();
                        } else {
                            changes = entity
                                    .getCollection()
                                    .updateMany( session, BsonDocument.parse( filter ), List.of( BsonDocument.parse( operations.get( 0 ) ) ) )
                                    .getModifiedCount();
                        }

                    }
                    break;

                case DELETE:
                    if ( !dataContext.getParameterValues().isEmpty() ) {
                        // prepared
                        MongoDynamic filterUtil = new MongoDynamic( BsonDocument.parse( filter ), bucket );
                        List<? extends WriteModel<Document>> filters;
                        if ( onlyOne ) {
                            filters = filterUtil.getAll( dataContext.getParameterValues(), DeleteOneModel::new );
                        } else {
                            filters = filterUtil.getAll( dataContext.getParameterValues(), DeleteManyModel::new );
                        }

                        changes = entity.getCollection().bulkWrite( session, filters ).getDeletedCount();
                    } else {
                        // direct
                        if ( onlyOne ) {
                            changes = entity
                                    .getCollection()
                                    .deleteOne( session, BsonDocument.parse( filter ) )
                                    .getDeletedCount();
                        } else {
                            changes = entity
                                    .getCollection()
                                    .deleteMany( session, BsonDocument.parse( filter ) )
                                    .getDeletedCount();
                        }
                    }
                    break;

                case MERGE:
                    throw new GenericRuntimeException( "MERGE IS NOT SUPPORTED" );
            }
            return changes;

        }

    }

}
