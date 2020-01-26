/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.util.CassandraTypesUtils;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.trace.PolyphenyDbTrace;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.slf4j.Logger;


/**
 * Schema mapped onto a Cassandra column family
 */
@Slf4j
public class CassandraSchema extends AbstractSchema {

//    @Getter
    final CqlSession session;
    final String keyspace;
    private final SchemaPlus parentSchema;
    final String name;

    public CqlSession getSession() {
        log.info( "GetSession call" );
        return this.session;
    }

    @Getter
    private final CassandraConvention convention;

    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    private static final int DEFAULT_CASSANDRA_PORT = 9042;


    /**
     * Creates a Cassandra schema.
     *
     * @param host Cassandra host, e.g. "localhost"
     * @param keyspace Cassandra keyspace name, e.g. "twissandra"
     */
    public CassandraSchema( String host, String keyspace, SchemaPlus parentSchema, String name ) {
        this( host, DEFAULT_CASSANDRA_PORT, keyspace, null, null, parentSchema, name );
    }


    /**
     * Creates a Cassandra schema.
     *
     * @param host Cassandra host, e.g. "localhost"
     * @param port Cassandra port, e.g. 9042
     * @param keyspace Cassandra keyspace name, e.g. "twissandra"
     */
    public CassandraSchema( String host, int port, String keyspace, SchemaPlus parentSchema, String name ) {
        this( host, port, keyspace, null, null, parentSchema, name );
    }


    /**
     * Creates a Cassandra schema.
     *
     * @param host Cassandra host, e.g. "localhost"
     * @param keyspace Cassandra keyspace name, e.g. "twissandra"
     * @param username Cassandra username
     * @param password Cassandra password
     */
    public CassandraSchema( String host, String keyspace, String username, String password, SchemaPlus parentSchema, String name ) {
        this( host, DEFAULT_CASSANDRA_PORT, keyspace, null, null, parentSchema, name );
    }


    /**
     * Creates a Cassandra schema.
     *
     * @param host Cassandra host, e.g. "localhost"
     * @param port Cassandra port, e.g. 9042
     * @param keyspace Cassandra keyspace name, e.g. "twissandra"
     * @param username Cassandra username
     * @param password Cassandra password
     */
    public CassandraSchema( String host, int port, String keyspace, String username, String password, SchemaPlus parentSchema, String name ) {
        super();

        throw new RuntimeException( "THIS CONSTRUCTOR IS CURRENTLY NOT USABLE! Call Jan." );
    }

    public CassandraSchema( CqlSession session, String keyspace, SchemaPlus parentSchema, String name, CassandraConvention convention ) {
        super();
        this.session = session;
        this.keyspace = keyspace;
        this.parentSchema = parentSchema;
        this.name = name;
        this.convention = convention;
    }

    public static CassandraSchema create( SchemaPlus parentSchema, String name, CqlSession session, String keyspace, CassandraPhysicalNameProvider physicalNameProvider ) {
        final Expression expression = Schemas.subSchemaExpression( parentSchema, name, CassandraSchema.class );
        final CassandraConvention convention = new CassandraConvention( name, expression, physicalNameProvider );
        return new CassandraSchema( session, keyspace, parentSchema, name, convention );
    }


    RelProtoDataType getRelDataType( String columnFamily, boolean view ) {
        log.info( "getRelDataType: {}", columnFamily );
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( this.name );
        qualifiedNames.add( columnFamily );
        String physicalTableName = this.convention.physicalNameProvider.getPhysicalTableName( qualifiedNames );
        Map<CqlIdentifier, ColumnMetadata> columns;
        if ( view ) {
            throw new RuntimeException( "Views are currently broken." );
        } else {
            columns = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get().getColumns();
        }

        // Temporary type factory, just for the duration of this method. Allowable because we're creating a proto-type, not a type; before being used, the proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for ( Entry<CqlIdentifier, ColumnMetadata> column : columns.entrySet() ) {
            final String columnName = column.getKey().toString();
            final DataType type = column.getValue().getType();

            // TODO: This mapping of types can be done much better
            SqlTypeName typeName = CassandraTypesUtils.getSqlTypeName( type );

            fieldInfo.add( columnName, typeFactory.createSqlType( typeName ) ).nullable( true );
        }

        return RelDataTypeImpl.proto( fieldInfo.build() );
    }


    /**
     * Get all primary key columns from the underlying CQL table
     *
     * @return A list of field names that are part of the partition and clustering keys
     */
    Pair<List<String>, List<String>> getKeyFields( String columnFamily, boolean view ) {
        RelationMetadata relation;
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( this.name );
        qualifiedNames.add( columnFamily );
        String physicalTableName = this.convention.physicalNameProvider.getPhysicalTableName( qualifiedNames );
        if ( view ) {
            relation = getKeyspace().getView( "\"" + physicalTableName + "\"" ).get();
        } else {
            relation = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get();
        }

        List<ColumnMetadata> partitionKey = relation.getPartitionKey();
        List<String> pKeyFields = new ArrayList<>();
        for ( ColumnMetadata column : partitionKey ) {
            pKeyFields.add( column.getName().toString() );
        }

        Map<ColumnMetadata, ClusteringOrder> clusteringKey = relation.getClusteringColumns();
        List<String> cKeyFields = new ArrayList<>();
        for ( Entry<ColumnMetadata, ClusteringOrder> column : clusteringKey.entrySet() ) {
            cKeyFields.add( column.getKey().toString() );
        }

        return Pair.of( ImmutableList.copyOf( pKeyFields ), ImmutableList.copyOf( cKeyFields ) );
    }


    /**
     * Get the collation of all clustering key columns.
     *
     * @return A RelCollations representing the collation of all clustering keys
     */
    public List<RelFieldCollation> getClusteringOrder( String columnFamily, boolean view ) {
        RelationMetadata relation;
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( this.name );
        qualifiedNames.add( columnFamily );
        String physicalTableName = this.convention.physicalNameProvider.getPhysicalTableName( qualifiedNames );
        if ( view ) {
//            throw new RuntimeException( "Views are currently broken." );
            relation = getKeyspace().getView( "\"" + physicalTableName + "\"" ).get();
        } else {
            relation = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get();
        }

        Map<ColumnMetadata, ClusteringOrder> clusteringOrder = relation.getClusteringColumns();
        List<RelFieldCollation> keyCollations = new ArrayList<>();

        int i = 0;
        for ( Entry<ColumnMetadata, ClusteringOrder> order : clusteringOrder.entrySet() ) {
            RelFieldCollation.Direction direction;
            switch ( order.getValue() ) {
                case DESC:
                    direction = RelFieldCollation.Direction.DESCENDING;
                    break;
                case ASC:
                default:
                    direction = RelFieldCollation.Direction.ASCENDING;
                    break;
            }
            keyCollations.add( new RelFieldCollation( i, direction ) );
            i++;
        }

        return keyCollations;
    }


    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for ( Entry<CqlIdentifier, TableMetadata> table : getKeyspace().getTables().entrySet() ) {
            builder.put( table.getKey().toString(), new CassandraTable( this, table.getKey().toString() ) );

            // TODO JS: Fix the view situation!
            /*for ( MaterializedViewMetadata view : table.getValue().getViews() ) {
                String viewName = view.getName();
                builder.put( viewName, new CassandraTable( this, viewName, true ) );
            }*/
        }
        return builder.build();
    }


    private KeyspaceMetadata getKeyspace() {
        Optional<KeyspaceMetadata> metadata = session.getMetadata().getKeyspace( keyspace );
        if (metadata.isPresent()) {
            return metadata.get();
        } else {
            throw new RuntimeException( "There is no metadata." );
        }
    }
}

