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

package org.polypheny.db.adapter.jdbc;


import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.Schema.TableType;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.language.util.SqlString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Queryable that gets its data from a table within a JDBC connection.
 *
 * The idea is not to read the whole table, however. The idea is to use this as a building block for a query, by
 * applying Queryable operators such as {@link org.apache.calcite.linq4j.Queryable#where(org.apache.calcite.linq4j.function.Predicate2)}.
 * The resulting queryable can then be converted to a SQL query, which can be executed efficiently on the JDBC server.
 */
public class JdbcTable extends AbstractQueryableTable implements TranslatableTable, ScannableTable, ModifiableTable {

    private AlgProtoDataType protoRowType;
    private JdbcSchema jdbcSchema;

    private final String physicalSchemaName;
    private final String physicalTableName;
    private final List<String> physicalColumnNames;

    private final String logicalSchemaName;
    private final String logicalTableName;
    private final List<String> logicalColumnNames;

    private final TableType jdbcTableType;


    public JdbcTable(
            JdbcSchema jdbcSchema,
            String logicalSchemaName,
            String logicalTableName,
            List<String> logicalColumnNames,
            TableType jdbcTableType,
            AlgProtoDataType protoRowType,
            String physicalSchemaName,
            String physicalTableName,
            List<String> physicalColumnNames,
            Long tableId ) {
        super( Object[].class );
        this.jdbcSchema = jdbcSchema;
        this.logicalSchemaName = logicalSchemaName;
        this.logicalTableName = logicalTableName;
        this.logicalColumnNames = logicalColumnNames;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physicalTableName;
        this.physicalColumnNames = physicalColumnNames;
        this.jdbcTableType = Objects.requireNonNull( jdbcTableType );
        this.protoRowType = protoRowType;
        this.tableId = tableId;
    }


    public String toString() {
        return "JdbcTable {" + physicalSchemaName + "." + physicalTableName + "}";
    }


    @Override
    public TableType getJdbcTableType() {
        return jdbcTableType;
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses( final JavaTypeFactory typeFactory ) {
        final AlgDataType rowType = protoRowType.apply( typeFactory );
        return Lists.transform( rowType.getFieldList(), f -> {
            final AlgDataType type = f.getType();
            final Class clazz = (Class) typeFactory.getJavaClass( type );
            final ColumnMetaData.Rep rep = Util.first( ColumnMetaData.Rep.of( clazz ), ColumnMetaData.Rep.OBJECT );
            return Pair.of( rep, type.getPolyType().getJdbcOrdinal() );
        } );
    }


    SqlString generateSql() {
        List<SqlNode> pcnl = Expressions.list();
        for ( String str : physicalColumnNames ) {
            pcnl.add( new SqlIdentifier( Arrays.asList( physicalTableName, str ), ParserPos.ZERO ) );
        }
        //final SqlNodeList selectList = new SqlNodeList( Collections.singletonList( SqlIdentifier.star( SqlParserPos.ZERO ) ), SqlParserPos.ZERO );
        final SqlNodeList selectList = new SqlNodeList( pcnl, ParserPos.ZERO );
        SqlIdentifier physicalTableName = new SqlIdentifier( Arrays.asList( physicalSchemaName, this.physicalTableName ), ParserPos.ZERO );
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
        return new SqlIdentifier( Arrays.asList( physicalSchemaName, physicalTableName ), ParserPos.ZERO );
    }


    public SqlIdentifier physicalColumnName( String logicalColumnName ) {
        String physicalName = physicalColumnNames.get( logicalColumnNames.indexOf( logicalColumnName ) );
        return new SqlIdentifier( Arrays.asList( physicalName ), ParserPos.ZERO );
    }


    public boolean hasPhysicalColumnName( String logicalColumnName ) {
        return logicalColumnNames.contains( logicalColumnName );
    }


    public SqlNodeList getNodeList() {
        List<SqlNode> pcnl = Expressions.list();
        int i = 0;
        for ( String str : physicalColumnNames ) {
            SqlNode[] operands = new SqlNode[]{
                    new SqlIdentifier( Arrays.asList( physicalSchemaName, physicalTableName, str ), ParserPos.ZERO ),
                    new SqlIdentifier( Arrays.asList( logicalColumnNames.get( i++ ) ), ParserPos.ZERO )
            };
            pcnl.add( new SqlBasicCall( (SqlOperator) OperatorRegistry.get( OperatorName.AS ), operands, ParserPos.ZERO ) );
        }
        return new SqlNodeList( pcnl, ParserPos.ZERO );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        return new JdbcScan( context.getCluster(), algOptTable, this, jdbcSchema.getConvention() );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new JdbcTableQueryable<>( dataContext, schema, tableName );
    }


    @Override
    public Enumerable<Object[]> scan( DataContext root ) {
        final JavaTypeFactory typeFactory = root.getTypeFactory();
        final SqlString sql = generateSql();
        return ResultSetEnumerable.of(
                jdbcSchema.getConnectionHandler( root ),
                sql.getSql(),
                JdbcUtils.ObjectArrayRowBuilder.factory( fieldClasses( typeFactory ) ) );
    }


    @Override
    public Collection getModifiableCollection() {
        throw new RuntimeException( "getModifiableCollection() is not implemented for JDBC adapter!" );
    }


    @Override
    public Modify toModificationAlg(
            AlgOptCluster cluster,
            AlgOptTable table,
            CatalogReader catalogReader,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        jdbcSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                input,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
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
    private class JdbcTableQueryable<T> extends AbstractTableQueryable<T> {

        JdbcTableQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
            super( dataContext, schema, JdbcTable.this, tableName );
        }


        @Override
        public String toString() {
            return "JdbcTableQueryable {table: " + physicalSchemaName + "." + tableName + "}";
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

