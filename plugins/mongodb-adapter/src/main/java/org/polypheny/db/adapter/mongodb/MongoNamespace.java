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


import com.google.common.collect.ImmutableSet;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.mongodb.MongoPlugin.MongoStore;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.SchemaVersion;


/**
 * Schema mapped onto a directory of MONGO files. Each table in the schema is a MONGO file in that directory.
 */
public class MongoNamespace implements Namespace, Schema, Expressible {

    @Getter
    final MongoDatabase database;

    @Getter
    private final Convention convention = MongoAlg.CONVENTION;

    @Getter
    private final Map<String, Entity> tableMap = new HashMap<>();

    @Getter
    private final Map<String, Entity> collectionMap = new HashMap<>();
    private final MongoClient connection;
    private final TransactionProvider transactionProvider;
    @Getter
    private final GridFSBucket bucket;
    @Getter
    private final MongoStore store;
    @Getter
    private final long id;


    /**
     * Creates a MongoDB schema.
     *
     * @param database Mongo database name, e.g. "foodmart"
     * @param transactionProvider
     * @param mongoStore
     */
    public MongoNamespace( long id, String database, MongoClient connection, TransactionProvider transactionProvider, MongoStore mongoStore ) {
        this.id = id;
        this.transactionProvider = transactionProvider;
        this.connection = connection;
        this.database = this.connection.getDatabase( database );
        this.bucket = GridFSBuckets.create( this.database, database );
        this.store = mongoStore;
    }


    public MongoEntity createEntity( StoreCatalog catalog, PhysicalEntity physical ) {
        return new MongoEntity( physical, this, transactionProvider );
    }


    @Override
    public Namespace getSubNamespace( String name ) {
        return null;
    }


    @Override
    public Set<String> getSubNamespaceNames() {
        return ImmutableSet.of();
    }


    @Override
    public CatalogEntity getEntity( String name ) {
        return null;
    }


    @Override
    public Set<String> getEntityNames() {
        return null;
    }


    @Override
    public AlgProtoDataType getType( String name ) {
        return null;
    }


    @Override
    public Set<String> getTypeNames() {
        return null;
    }


    @Override
    public Collection<Function> getFunctions( String name ) {
        return null;
    }


    @Override
    public Set<String> getFunctionNames() {
        return null;
    }


    @Override
    public Expression getExpression( Snapshot snapshot, long id ) {
        return null;
    }


    @Override
    public boolean isMutable() {
        return false;
    }


    @Override
    public Namespace snapshot( SchemaVersion version ) {
        return null;
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    /*public Entity createCollection( LogicalCollection catalogEntity, CatalogCollectionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        AlgDataType type = typeFactory.createPolyType( PolyType.DOCUMENT );
        fieldInfo.add( "d", "d", type ).nullable( false );

        MongoEntity table = new MongoEntity( catalogEntity, this, AlgDataTypeImpl.proto( fieldInfo.build() ), transactionProvider, partitionPlacement.adapter, partitionPlacement );

        tableMap.put( catalogEntity.name + "_" + partitionPlacement.id, table );
        return table;
    }*/

}

