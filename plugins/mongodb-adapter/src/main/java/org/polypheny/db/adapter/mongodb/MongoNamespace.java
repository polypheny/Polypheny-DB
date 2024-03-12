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


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.mongodb.MongoPlugin.MongoStore;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;


/**
 * Namespace representing a physical namespace according to the MongoDB database and holding the connection to MongoDB.
 */
@EqualsAndHashCode(callSuper = true)
@Value
@Getter
public class MongoNamespace extends Namespace implements Expressible {

    public MongoDatabase database;

    Convention convention = MongoAlg.CONVENTION;

    MongoClient connection;

    TransactionProvider transactionProvider;

    GridFSBucket bucket;

    MongoStore store;



    /**
     * Creates a MongoDB namespace.
     *
     * @param database Mongo database name, e.g. "foodmart"
     */
    public MongoNamespace( long id, String database, MongoClient connection, TransactionProvider transactionProvider, MongoStore mongoStore ) {
        super( id, mongoStore.getAdapterId() );
        this.transactionProvider = transactionProvider;
        this.connection = connection;
        this.database = this.connection.getDatabase( database );
        this.bucket = GridFSBuckets.create( this.database, database );
        this.store = mongoStore;
    }


    public MongoEntity createEntity( PhysicalEntity physical, List<? extends PhysicalField> fields ) {
        return new MongoEntity( physical, fields, this, transactionProvider );
    }


    @Override
    public Expression asExpression() {
        return null;
    }


}

