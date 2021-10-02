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


import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.DeleteManyModel;
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
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.mongodb.MongoEnumerator.IterWrapper;
import org.polypheny.db.adapter.mongodb.util.MongoDynamic;
import org.polypheny.db.adapter.mongodb.util.MongoTypeUtil;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Util;


/**
 * Table based on a MongoDB collection.
 */
public class MongoTable extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    @Getter
    private final String collectionName;
    @Getter
    private final RelProtoDataType protoRowType;
    @Getter
    private final MongoSchema mongoSchema;
    @Getter
    private final MongoCollection<Document> collection;
    @Getter
    private final CatalogTable catalogTable;
    @Getter
    private final TransactionProvider transactionProvider;
    @Getter
    private final int storeId;


    /**
     * Creates a MongoTable.
     */
    MongoTable( CatalogTable catalogTable, MongoSchema schema, RelProtoDataType proto, TransactionProvider transactionProvider, int storeId, CatalogPartitionPlacement partitionPlacement ) {
        super( Object[].class );
        this.collectionName = MongoStore.getPhysicalTableName( catalogTable.id, partitionPlacement.partitionId );
        this.transactionProvider = transactionProvider;
        this.catalogTable = catalogTable;
        this.protoRowType = proto;
        this.mongoSchema = schema;
        this.collection = schema.database.getCollection( collectionName );
        this.storeId = storeId;
    }


    public String toString() {
        return "MongoTable {" + collectionName + "}";
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new MongoQueryable<>( dataContext, schema, this, tableName );
    }


    @Override
    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        final RelOptCluster cluster = context.getCluster();
        return new MongoTableScan( cluster, cluster.traitSetOf( MongoRel.CONVENTION ), relOptTable, this, null );
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
    private Enumerable<Object> find( MongoDatabase mongoDb, MongoTable table, String filterJson, String projectJson, List<Entry<String, Class>> fields, List<Entry<String, Class>> arrayFields ) {
        final MongoCollection collection = mongoDb.getCollection( collectionName );
        final Bson filter = filterJson == null ? null : BsonDocument.parse( filterJson );
        final Bson project = projectJson == null ? null : BsonDocument.parse( projectJson );
        final Function1<Document, Object> getter = MongoEnumerator.getter( fields, arrayFields );

        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                @SuppressWarnings("unchecked") final FindIterable<Document> cursor = collection.find( filter ).projection( project );
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
     * @param session
     * @param mongoDb MongoDB connection
     * @param fields List of fields to project; or null to return map
     * @param operations One or more JSON strings
     * @param parameterValues
     * @param filter
     * @return Enumerator of results
     */
    private Enumerable<Object> aggregate(
            ClientSession session,
            final MongoDatabase mongoDb,
            MongoTable table,
            final List<Entry<String, Class>> fields,
            List<Entry<String, Class>> arrayFields,
            final List<String> operations,
            Map<Long, Object> parameterValues,
            BsonDocument filter,
            List<BsonDocument> preOps,
            List<String> logicalCols ) {
        final List<BsonDocument> list = new ArrayList<>();

        if ( parameterValues.size() == 0 ) {
            // direct query
            preOps.forEach( op -> list.add( new BsonDocument( "$project", op ) ) );

            if ( !filter.isEmpty() ) {
                list.add( new BsonDocument( "$match", filter ) );
            }

            for ( String operation : operations ) {
                list.add( BsonDocument.parse( operation ) );
            }
        } else {
            // prepared
            preOps.stream()
                    .map( op -> new MongoDynamic( new BsonDocument( "$addFields", op ), mongoSchema.getBucket() ) )
                    .forEach( util -> list.add( util.insert( parameterValues ) ) );

            if ( !filter.isEmpty() ) {
                MongoDynamic util = new MongoDynamic( filter, getMongoSchema().getBucket() );
                list.add( new BsonDocument( "$match", util.insert( parameterValues ) ) );
            }

            for ( String operation : operations ) {
                MongoDynamic opUtil = new MongoDynamic( BsonDocument.parse( operation ), getMongoSchema().getBucket() );
                list.add( opUtil.insert( parameterValues ) );
            }

        }

        if ( logicalCols.size() != 0 ) {
            list.add( 0, MongoTypeUtil.getPhysicalProjections( logicalCols, catalogTable ) );
        }

        final Function1<Document, Object> getter = MongoEnumerator.getter( fields, arrayFields );
        //list.forEach( el -> System.out.println( el.toBsonDocument().toJson( JsonWriterSettings.builder().outputMode( JsonMode.SHELL ).build() ) ) );
        return new AbstractEnumerable<Object>() {
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
    public TableModify toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            CatalogReader catalogReader,
            RelNode child,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        mongoSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify(
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


    /**
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a {@link org.polypheny.db.adapter.mongodb.MongoTable}.
     *
     * @param <T> element type
     */
    public static class MongoQueryable<T> extends AbstractTableQueryable<T> {

        MongoQueryable( DataContext dataContext, SchemaPlus schema, MongoTable table, String tableName ) {
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


        private MongoTable getTable() {
            return (MongoTable) table;
        }


        /**
         * Called via code-generation.
         *
         * @see MongoMethod#MONGO_QUERYABLE_AGGREGATE
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> aggregate( List<Map.Entry<String, Class>> fields, List<Map.Entry<String, Class>> arrayClass, List<String> operations, String filter, List<String> preProjections, List<String> logicalCols ) {
            ClientSession session = getTable().getTransactionProvider().getSession( dataContext.getStatement().getTransaction().getXid() );

            Map<Long, Object> values = new HashMap<>();
            if ( dataContext.getParameterValues().size() == 1 ) {
                values = dataContext.getParameterValues().get( 0 );
            }

            return getTable().aggregate(
                    session,
                    getMongoDb(),
                    getTable(),
                    fields,
                    arrayClass,
                    operations,
                    values,
                    BsonDocument.parse( filter ),
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
         * This methods handles direct DMLs(which already have the values)
         *
         * @param operation which defines which kind the DML is
         * @param filter filter operations
         * @param operations the collection of values to insert
         * @return the enumerable which holds the result of the operation
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> handleDirectDML( Operation operation, String filter, List<String> operations ) {
            MongoTable mongoTable = getTable();
            PolyXid xid = dataContext.getStatement().getTransaction().getXid();
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( mongoTable.getStoreId() ) );
            ClientSession session = mongoTable.getTransactionProvider().startTransaction( xid );
            GridFSBucket bucket = mongoTable.getMongoSchema().getBucket();

            long changes = 0;

            switch ( operation ) {
                case INSERT:
                    if ( dataContext.getParameterValues().size() != 0 ) {
                        assert operations.size() == 1;
                        // prepared
                        MongoDynamic util = new MongoDynamic( BsonDocument.parse( operations.get( 0 ) ), bucket );
                        List<Document> inserts = util.getAll( dataContext.getParameterValues() );
                        mongoTable.getCollection().insertMany( session, inserts );
                        changes = inserts.size();
                    } else {
                        // direct
                        List<Document> docs = operations.stream().map( BsonDocument::parse ).map( MongoTypeUtil::asDocument ).collect( Collectors.toList() );
                        mongoTable.getCollection().insertMany( session, docs );
                        changes = docs.size();
                    }
                    break;

                case UPDATE:
                    assert operations.size() == 1;
                    if ( dataContext.getParameterValues().size() != 0 ) {
                        // prepared
                        MongoDynamic filterUtil = new MongoDynamic( BsonDocument.parse( filter ), bucket );
                        MongoDynamic docUtil = new MongoDynamic( BsonDocument.parse( operations.get( 0 ) ), bucket );
                        for ( Map<Long, Object> parameterValue : dataContext.getParameterValues() ) {
                            changes += mongoTable.getCollection().updateMany( session, filterUtil.insert( parameterValue ), Collections.singletonList( docUtil.insert( parameterValue ) ) ).getModifiedCount();
                        }
                    } else {
                        // direct
                        changes = mongoTable.getCollection().updateMany( session, BsonDocument.parse( filter ), Collections.singletonList( BsonDocument.parse( operations.get( 0 ) ) ) ).getModifiedCount();
                    }
                    break;

                case DELETE:
                    if ( dataContext.getParameterValues().size() != 0 ) {
                        // prepared
                        MongoDynamic filterUtil = new MongoDynamic( BsonDocument.parse( filter ), bucket );
                        List<? extends WriteModel<Document>> filters = filterUtil.getAll( dataContext.getParameterValues(), DeleteManyModel::new );
                        changes = mongoTable.getCollection().bulkWrite( session, filters ).getDeletedCount();
                    } else {
                        // direct
                        changes = mongoTable.getCollection().deleteMany( session, BsonDocument.parse( filter ) ).getDeletedCount();
                    }
                    break;

                case MERGE:
                    throw new RuntimeException( "MERGE IS NOT SUPPORTED" );
            }

            long finalChanges = changes;
            return new AbstractEnumerable<Object>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return new IterWrapper( Collections.singletonList( (Object) finalChanges ).iterator() );
                }
            };
        }

    }

}
