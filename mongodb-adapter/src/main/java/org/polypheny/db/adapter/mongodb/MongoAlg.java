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


import com.mongodb.client.gridfs.GridFSBucket;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.util.Pair;


/**
 * Relational expression that uses Mongo calling convention.
 */
public interface MongoAlg extends AlgNode {

    void implement( Implementor implementor );

    /**
     * Calling convention for relational operations that occur in MongoDB.
     */
    Convention CONVENTION = MongoConvention.INSTANCE;//new Convention.Impl( "MONGO", MongoRel.class );


    /**
     * Callback for the implementation process that converts a tree of {@link MongoAlg} nodes into a MongoDB query.
     */
    class Implementor extends AlgShuttleImpl implements Serializable {

        final List<Pair<String, String>> list = new ArrayList<>();
        public List<BsonDocument> operations = new ArrayList<>();
        public BsonArray filter = new BsonArray();
        @Getter
        @Setter
        public GridFSBucket bucket;
        public List<BsonDocument> preProjections = new ArrayList<>();

        // holds the logical names which where used in a DQL
        // and need to be projected beforehand from their physical names
        public Set<String> physicalMapper = new TreeSet<>();
        public boolean onlyOne = false;
        public boolean isDocumentUpdate = false;

        AlgOptTable table;
        @Setter
        @Getter
        public boolean hasProject = false;

        MongoEntity mongoEntity;
        @Setter
        @Getter
        private boolean isDML;

        @Getter
        @Setter
        private Operation operation;

        @Getter
        private AlgRecordType staticRowType;


        public Implementor() {
            isDML = false;
        }


        public Implementor( boolean isDML ) {
            this.isDML = isDML;
        }


        public void add( String findOp, String aggOp ) {
            list.add( Pair.of( findOp, aggOp ) );
        }


        public void visitChild( int ordinal, AlgNode input ) {
            assert ordinal == 0;
            ((MongoAlg) input).implement( this );
        }


        public void setStaticRowType( AlgRecordType staticRowType ) {
            if ( this.staticRowType != null ) {
                return;
            }
            if ( mongoEntity != null ) {
                this.staticRowType = MongoRowType.fromRecordType( staticRowType, mongoEntity );
            } else {
                this.staticRowType = staticRowType;
            }
        }


        public String getPhysicalName( String name ) {
            int index = mongoEntity.getCatalogTable().getColumnNames().indexOf( name );
            if ( index != -1 ) {
                return MongoStore.getPhysicalColumnName( name, mongoEntity.getCatalogTable().fieldIds.get( index ) );
            }
            throw new RuntimeException( "This column is not part of the table." );
        }


        public BsonDocument getFilter() {
            BsonDocument filter;
            if ( this.filter.size() == 1 ) {
                filter = this.filter.get( 0 ).asDocument();
            } else if ( this.filter.size() == 0 ) {
                filter = new BsonDocument();
            } else {
                filter = new BsonDocument( "$or", this.filter );
            }

            return filter;
        }


        public String getFilterSerialized() {
            return toJson( getFilter() );
        }


        public List<String> getPreProjects() {
            return preProjections.stream().map( Implementor::toJson ).collect( Collectors.toList() );
        }


        public List<String> getOperations() {
            return operations.stream().map( Implementor::toJson ).collect( Collectors.toList() );
        }


        public static String toJson( BsonDocument doc ) {
            return doc.toJson( JsonWriterSettings.builder().outputMode( JsonMode.EXTENDED ).build() );
        }


        @Override
        public AlgNode visit( LogicalProject project ) {
            super.visit( project );

            return project;
        }


        @Override
        public AlgNode visit( Scan scan ) {
            super.visit( scan );

            return scan;
        }


        public List<String> getNecessaryPhysicalFields() {
            return new ArrayList<>( physicalMapper );
        }


        public List<String> reorderPhysical() {
            // this is only needed if there is a basic scan without project or group,
            // where we cannot be sure if the fields are all ordered as intended
            assert table.getRowType().getFieldCount() == physicalMapper.size();
            return table.getRowType().getFieldNames();
        }

    }

}

