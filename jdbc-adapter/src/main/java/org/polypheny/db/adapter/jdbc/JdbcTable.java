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
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.Schema.TableType;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.sql.SqlBasicCall;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlSelect;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.util.SqlString;
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

    private RelProtoDataType protoRowType;
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
            RelProtoDataType protoRowType,
            String physicalSchemaName,
            String physicalTableName,
            List<String> physicalColumnNames ) {
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

    }


    public String toString() {
        return "JdbcTable {" + physicalSchemaName + "." + physicalTableName + "}";
    }


    @Override
    public TableType getJdbcTableType() {
        return jdbcTableType;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses( final JavaTypeFactory typeFactory ) {
        final RelDataType rowType = protoRowType.apply( typeFactory );
        return Lists.transform( rowType.getFieldList(), f -> {
            final RelDataType type = f.getType();
            final Class clazz = (Class) typeFactory.getJavaClass( type );
            final ColumnMetaData.Rep rep = Util.first( ColumnMetaData.Rep.of( clazz ), ColumnMetaData.Rep.OBJECT );
            return Pair.of( rep, type.getPolyType().getJdbcOrdinal() );
        } );
    }


    SqlString generateSql() {
        List<SqlNode> pcnl = Expressions.list();
        for ( String str : physicalColumnNames ) {
            pcnl.add( new SqlIdentifier( Arrays.asList( physicalSchemaName, physicalTableName, str ), SqlParserPos.ZERO ) );
        }
        //final SqlNodeList selectList = new SqlNodeList( Collections.singletonList( SqlIdentifier.star( SqlParserPos.ZERO ) ), SqlParserPos.ZERO );
        final SqlNodeList selectList = new SqlNodeList( pcnl, SqlParserPos.ZERO );
        SqlIdentifier physicalTableName = new SqlIdentifier( Arrays.asList( physicalSchemaName, this.physicalTableName ), SqlParserPos.ZERO );
        SqlSelect node = new SqlSelect(
                SqlParserPos.ZERO,
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
        return new SqlIdentifier( Arrays.asList( physicalSchemaName, physicalTableName ), SqlParserPos.ZERO );
    }


    public SqlIdentifier physicalColumnName( String logicalColumnName ) {
        String physicalName = physicalColumnNames.get( logicalColumnNames.indexOf( logicalColumnName ) );
        return new SqlIdentifier( Arrays.asList( physicalName ), SqlParserPos.ZERO );
    }


    public SqlNodeList getNodeList() {
        List<SqlNode> pcnl = Expressions.list();
        int i = 0;
        for ( String str : physicalColumnNames ) {
            SqlNode[] operands = new SqlNode[]{
                    new SqlIdentifier( Arrays.asList( physicalSchemaName, physicalTableName, str ), SqlParserPos.ZERO ),
                    new SqlIdentifier( Arrays.asList( logicalColumnNames.get( i++ ) ), SqlParserPos.ZERO )
            };
            pcnl.add( new SqlBasicCall( SqlStdOperatorTable.AS, operands, SqlParserPos.ZERO ) );
        }
        return new SqlNodeList( pcnl, SqlParserPos.ZERO );
    }


    @Override
    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        return new JdbcTableScan( context.getCluster(), relOptTable, this, jdbcSchema.getConvention() );
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
    public TableModify toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            CatalogReader catalogReader,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        jdbcSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify(
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

