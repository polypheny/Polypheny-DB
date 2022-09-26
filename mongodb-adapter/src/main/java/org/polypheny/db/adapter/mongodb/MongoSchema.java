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


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;


/**
 * Schema mapped onto a directory of MONGO files. Each table in the schema is a MONGO file in that directory.
 */
public class MongoSchema extends AbstractSchema {

    @Getter
    final MongoDatabase database;

    @Getter
    private final Convention convention = MongoAlg.CONVENTION;

    @Getter
    private final Map<String, Table> tableMap = new HashMap<>();

    @Getter
    private final Map<String, Table> collectionMap = new HashMap<>();
    private final MongoClient connection;
    private final TransactionProvider transactionProvider;
    @Getter
    private final GridFSBucket bucket;
    @Getter
    private final MongoStore store;


    /**
     * Creates a MongoDB schema.
     *
     * @param database Mongo database name, e.g. "foodmart"
     * @param transactionProvider
     * @param mongoStore
     */
    public MongoSchema( String database, MongoClient connection, TransactionProvider transactionProvider, MongoStore mongoStore ) {
        super();
        this.transactionProvider = transactionProvider;
        this.connection = connection;
        this.database = this.connection.getDatabase( database );
        this.bucket = GridFSBuckets.create( this.database, database );
        this.store = mongoStore;
    }


    private String buildDatabaseName( CatalogColumn column ) {
        return column.getDatabaseName() + "_" + column.getSchemaName() + "_" + column.name;
    }


    public MongoEntity createTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, int storeId, CatalogPartitionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            AlgDataType sqlType = catalogColumn.getAlgDataType( typeFactory );
            fieldInfo.add( catalogColumn.name, MongoStore.getPhysicalColumnName( catalogColumn.name, catalogColumn.id ), sqlType ).nullable( catalogColumn.nullable );
        }
        MongoEntity table = new MongoEntity( catalogTable, this, AlgDataTypeImpl.proto( fieldInfo.build() ), transactionProvider, storeId, partitionPlacement );

        tableMap.put( catalogTable.name + "_" + partitionPlacement.partitionId, table );
        return table;
    }


    public Table createCollection( CatalogCollection catalogEntity, CatalogCollectionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        AlgDataType type = typeFactory.createPolyType( PolyType.DOCUMENT );
        fieldInfo.add( "d", "d", type ).nullable( false );

        MongoEntity table = new MongoEntity( catalogEntity, this, AlgDataTypeImpl.proto( fieldInfo.build() ), transactionProvider, partitionPlacement.adapter, partitionPlacement );

        tableMap.put( catalogEntity.name + "_" + partitionPlacement.id, table );
        return table;
    }

}

