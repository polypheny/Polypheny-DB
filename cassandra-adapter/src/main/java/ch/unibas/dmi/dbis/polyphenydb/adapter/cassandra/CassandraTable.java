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


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.AbstractQueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.CatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify.Operation;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;


/**
 * Table based on a Cassandra column family
 */
public class CassandraTable extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    RelProtoDataType protoRowType;
    Pair<List<String>, List<String>> keyFields;
    List<RelFieldCollation> clusteringOrder;
    private final CassandraSchema cassandraSchema;
    private final String columnFamily;
    private final String physicalName;
    private final boolean view;


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


    public CassandraTable( CassandraSchema cassandraSchema, String columnFamily ) {
        this( cassandraSchema, columnFamily, false );
    }


    public String toString() {
        return "CassandraTable {" + columnFamily + "}";
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        if ( protoRowType == null ) {
            protoRowType = cassandraSchema.getRelDataType( columnFamily, view );
        }
        return protoRowType.apply( typeFactory );
    }


    public Pair<List<String>, List<String>> getKeyFields() {
        if ( keyFields == null ) {
            keyFields = cassandraSchema.getKeyFields( columnFamily, view );
        }
        return keyFields;
    }


    public List<RelFieldCollation> getClusteringOrder() {
        if ( clusteringOrder == null ) {
            clusteringOrder = cassandraSchema.getClusteringOrder( columnFamily, view );
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
     * @param selectFields
     * @param predicates A list of predicates which should be used in the query
     * @param order
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
        for (Map.Entry<String, ClusteringOrder> entry: order) {
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
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a {@link ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTable}.
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
         * @see ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
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

