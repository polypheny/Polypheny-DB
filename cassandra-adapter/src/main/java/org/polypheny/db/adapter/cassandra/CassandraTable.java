/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelFieldCollation;
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
import org.polypheny.db.util.Pair;


/**
 * Table based on a Cassandra column family
 */
public class CassandraTable extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    RelProtoDataType protoRowType;
    Pair<List<String>, List<String>> keyFields;
    Pair<List<String>, List<String>> physicalKeyFields;
    List<RelFieldCollation> clusteringOrder;
    private final CassandraSchema cassandraSchema;
    private final String columnFamily;
    private final String physicalName;
    private final boolean view;

//    private final String physicalTableName;

//    private final String logicalTableName;


    public CassandraTable( CassandraSchema cassandraSchema, String columnFamily, boolean view ) {
        super( Object[].class );
        this.cassandraSchema = cassandraSchema;
        this.columnFamily = columnFamily;
        this.view = view;

        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( cassandraSchema.name );
        qualifiedNames.add( columnFamily );
        this.physicalName = cassandraSchema.getConvention().physicalNameProvider.getPhysicalTableName( qualifiedNames );
    }


    public CassandraTable( CassandraSchema cassandraSchema, String columnFamily, String physicalName, boolean view ) {
        super( Object[].class );
        this.cassandraSchema = cassandraSchema;
        this.columnFamily = columnFamily;
        this.view = view;
        this.physicalName = physicalName;
    }


    public CassandraTable( CassandraSchema cassandraSchema, String columnFamily ) {
        this( cassandraSchema, columnFamily, false );
    }


    public String toString() {
        return "CassandraTable {" + columnFamily + "}";
    }


    public CassandraConvention getUnderlyingConvention() {
        return this.cassandraSchema.getConvention();
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        if ( protoRowType == null ) {
            protoRowType = cassandraSchema.getRelDataType( physicalName, view );
        }
        return protoRowType.apply( typeFactory );
    }


    public Pair<List<String>, List<String>> getKeyFields() {
        if ( keyFields == null ) {
            keyFields = cassandraSchema.getKeyFields( physicalName, view );
        }
        return keyFields;
    }


    public Pair<List<String>, List<String>> getPhysicalKeyFields() {
        if ( physicalKeyFields == null ) {
            physicalKeyFields = cassandraSchema.getPhysicalKeyFields( physicalName, view );
        }
        return physicalKeyFields;
    }


    public List<RelFieldCollation> getClusteringOrder() {
        if ( clusteringOrder == null ) {
            clusteringOrder = cassandraSchema.getClusteringOrder( physicalName, view );
        }
        return clusteringOrder;
    }


    public Enumerable<Object> query( final CqlSession session ) {
        return query( session, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), 0, -1 );
    }


    /**
     * Executes a CQL query on the underlying table.
     *
     * @param session Cassandra session
     * @param fields List of fields to project
     * @param predicates A list of predicates which should be used in the query
     * @return Enumerator of results
     */
    public Enumerable<Object> query(
            final CqlSession session,
            List<Entry<String, Class>> fields,
            final List<Selector> selectFields,
            List<Relation> predicates,
            List<Entry<String, ClusteringOrder>> order,
            final Integer offset,
            final Integer fetch ) {
        // Build the type of the resulting row based on the provided fields
        /*final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        final RelDataType rowType = getRowType( typeFactory );

        Function1<String, Void> addField = fieldName -> {
            SqlTypeName typeName = rowType.getField( fieldName, true, false ).getType().getSqlTypeName();
            fieldInfo.add( fieldName, typeFactory.createSqlType( typeName ) ).nullable( true );
            return null;
        };*/

        SelectFrom selectFrom = QueryBuilder.selectFrom( columnFamily );

//        final RelProtoDataType resultRowType = RelDataTypeImpl.proto( fieldInfo.build() );

        Select select;
        // Construct the list of fields to project
        if ( selectFields.isEmpty() ) {
            select = selectFrom.all();
        } else {
            select = selectFrom.selectors( selectFields );
        }

        select = select.where( predicates );

        // FIXME js: Horrible hack, but hopefully works for now till I understand everything better.
        Map<String, ClusteringOrder> orderMap = new LinkedHashMap<>();
        for ( Map.Entry<String, ClusteringOrder> entry : order ) {
            orderMap.put( entry.getKey(), entry.getValue() );
        }

        select = select.orderBy( orderMap );
        int limit = offset;
        if ( fetch >= 0 ) {
            limit += fetch;
        }
        if ( limit > 0 ) {
            select = select.limit( limit );
        }

        select = select.allowFiltering();

        final SimpleStatement statement = select.build();

        return new CassandraEnumerable( session, statement.getQuery(), offset );
    }


    public Enumerable<Object> insert() {
        /*final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        final RelDataType rowType = getRowType( typeFactory );

        Function1<String, Void> addField = fieldName -> {
            SqlTypeName typeName = rowType.getField( fieldName, true, false ).getType().getSqlTypeName();
            fieldInfo.add( fieldName, typeFactory.createSqlType( typeName ) ).nullable( true );
            return null;
        };

        final RelProtoDataType resultRowType = RelDataTypeImpl.proto( fieldInfo.build() );*/

        return null;
    }


    CqlSession getSession() {
        return cassandraSchema.getSession();
    }


    String getColumnFamily() {
        return this.columnFamily;
    }


    String getPhysicalName() {
        return this.physicalName;
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new CassandraQueryable<>( dataContext, schema, this, tableName );
    }


    @Override
    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        final RelOptCluster cluster = context.getCluster();
        return new CassandraTableScan( cluster, cluster.traitSetOf( cassandraSchema.getConvention() ), relOptTable, this, null );
    }


    @Override
    public Collection getModifiableCollection() {
        return null;
    }


    @Override
    public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, CatalogReader catalogReader, RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
//        return new CassandraTableModify( cluster,  )
        cassandraSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify( cluster, cluster.traitSetOf( Convention.NONE ), table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
//        return new CassandraTableModify( cluster, cluster.traitSetOf( CassandraRel.CONVENTION ), table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened, this, this.columnFamily );
    }


    /**
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a {@link org.polypheny.db.adapter.cassandra.CassandraTable}.
     *
     * @param <T> element type
     */
    public class CassandraQueryable<T> extends AbstractTableQueryable<T> {

        public CassandraQueryable( DataContext dataContext, SchemaPlus schema, CassandraTable table, String tableName ) {
            super( dataContext, schema, table, tableName );
        }


        @Override
        public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable = (Enumerable<T>) getTable().query( getSession() );
            return enumerable.enumerator();
        }


        private CassandraTable getTable() {
            return (CassandraTable) table;
        }


        private CqlSession getSession() {
            return schema.unwrap( CassandraSchema.class ).session;
        }


        /**
         * Called via code-generation.
         *
         * @see org.polypheny.db.adapter.cassandra.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> query(
                List<Map.Entry<String, Class>> fields,
                List<Selector> selectFields,
                List<Relation> predicates,
                List<Map.Entry<String, ClusteringOrder>> order,
                Integer offset,
                Integer fetch ) {
            return getTable().query( cassandraSchema.getSession(), fields, selectFields, predicates, order, offset, fetch );
        }


        public Enumerable<Object> insert(
                String query ) {
            return CassandraEnumerable.of( getSession(), query );
        }
    }
}

