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

package org.polypheny.db.adapter.jdbc;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.impl.AbstractEntityQueryable;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.language.util.SqlString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Queryable that gets its data from a table within a JDBC connection.
 *
 * The idea is not to read the whole table, however. The idea is to use this as a building block for a query, by
 * applying Queryable operators such as {@link org.apache.calcite.linq4j.Queryable#where(org.apache.calcite.linq4j.function.Predicate2)}.
 * The resulting queryable can then be converted to a SQL query, which can be executed efficiently on the JDBC server.
 */
public class JdbcTable extends PhysicalTable implements TranslatableEntity, ScannableEntity, ModifiableTable, QueryableEntity {

    private JdbcSchema jdbcSchema;


    public JdbcTable(
            JdbcSchema jdbcSchema,
            PhysicalTable physical ) {
        super(
                physical.id,
                physical.allocationId,
                physical.logicalId,
                physical.name,
                physical.columns,
                physical.namespaceId,
                physical.namespaceName,
                physical.uniqueFieldIds,
                physical.adapterId );
        this.jdbcSchema = jdbcSchema;
    }


    public String toString() {
        return "JdbcTable {" + namespaceName + "." + name + "}";
    }


    private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses( final JavaTypeFactory typeFactory ) {
        final AlgDataType rowType = getTupleType();
        return rowType.getFields().stream().map( f -> {
            final AlgDataType type = f.getType();
            final Class<?> clazz = (Class<?>) typeFactory.getJavaClass( type );
            final Rep rep = Util.first( Rep.of( clazz ), Rep.OBJECT );
            return Pair.of( rep, type.getPolyType().getJdbcOrdinal() );
        } ).toList();
    }


    SqlString generateSql() {
        List<SqlNode> pcnl = Expressions.list();
        for ( PhysicalColumn column : columns ) {
            pcnl.add( new SqlIdentifier( Arrays.asList( name, column.name ), ParserPos.ZERO ) );
        }

        final SqlNodeList selectList = new SqlNodeList( pcnl, ParserPos.ZERO );
        SqlIdentifier physicalTableName = new SqlIdentifier( Arrays.asList( namespaceName, name ), ParserPos.ZERO );
        SqlSelect node = new SqlSelect(
                ParserPos.ZERO,
                SqlNodeList.EMPTY,
                selectList,
                physicalTableName,
                null,
                null,
                null,
                null,
                null,
                null,
                null );
        final SqlPrettyWriter writer = new SqlPrettyWriter( jdbcSchema.dialect );
        node.unparse( writer, 0, 0 );
        return writer.toSqlString();
    }


    public SqlIdentifier physicalTableName() {
        return new SqlIdentifier( Arrays.asList( namespaceName, name ), ParserPos.ZERO );
    }


    public SqlNodeList getNodeList() {
        List<SqlNode> pcnl = Expressions.list();
        for ( PhysicalColumn column : columns ) {
            SqlNode[] operands = new SqlNode[]{
                    new SqlIdentifier( Arrays.asList( namespaceName, name, column.name ), ParserPos.ZERO ),
                    new SqlIdentifier( Collections.singletonList( column.logicalName ), ParserPos.ZERO )
            };
            pcnl.add( new SqlBasicCall( (SqlOperator) OperatorRegistry.get( OperatorName.AS ), operands, ParserPos.ZERO ) );
        }
        return new SqlNodeList( pcnl, ParserPos.ZERO );
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        jdbcSchema.getConvention().register( cluster.getPlanner() );
        return new JdbcScan( cluster, this, jdbcSchema.getConvention() );
    }


    @Override
    public Queryable<PolyValue[]> asQueryable( DataContext dataContext, Snapshot snapshot ) {
        return new JdbcEntityQueryable<>( dataContext, snapshot, this );
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext root ) {
        final JavaTypeFactory typeFactory = root.getTypeFactory();
        final SqlString sql = generateSql();
        return ResultSetEnumerable.of(
                jdbcSchema.getConnectionHandler( root ),
                sql.getSql(),
                JdbcUtils.ObjectArrayRowBuilder.factory( fieldClasses( typeFactory ) ) );
    }


    @Override
    public Modify<?> toModificationTable(
            AlgCluster cluster,
            AlgTraitSet algTraits,
            Entity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList ) {
        jdbcSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster.traitSetOf( Convention.NONE ),
                table,
                input,
                operation,
                updateColumnList,
                sourceExpressionList );
    }


    public JdbcSchema getSchema() {
        return jdbcSchema;
    }


    // For unit testing only
    public void setSchema( JdbcSchema jdbcSchema ) {
        this.jdbcSchema = jdbcSchema;
    }


    /**
     * Enumerable that returns the contents of a {@link JdbcTable} by connecting to the JDBC data source.
     *
     * @param <T> element type
     */
    private class JdbcEntityQueryable<T> extends AbstractEntityQueryable<T, JdbcTable> {

        JdbcEntityQueryable( DataContext dataContext, Snapshot snapshot, JdbcTable entity ) {
            super( dataContext, snapshot, entity );
        }


        @Override
        public String toString() {
            return "JdbcTableQueryable {table: " + entity.namespaceName + "." + entity.name + "}";
        }


        @Override
        public Enumerator<T> enumerator() {
            final JavaTypeFactory typeFactory = dataContext.getTypeFactory();
            final SqlString sql = generateSql();
            //noinspection unchecked
            final Enumerable<T> enumerable = (Enumerable<T>) ResultSetEnumerable.of(
                    jdbcSchema.getConnectionHandler( dataContext ),
                    sql.getSql(),
                    JdbcUtils.ObjectArrayRowBuilder.factory( fieldClasses( typeFactory ) ) );
            return enumerable.enumerator();
        }

    }

}

