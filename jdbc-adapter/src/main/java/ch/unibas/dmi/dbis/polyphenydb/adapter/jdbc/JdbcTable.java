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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.AbstractQueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.CatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify.Operation;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.pretty.SqlPrettyWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlString;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
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


/**
 * Queryable that gets its data from a table within a JDBC connection.
 *
 * The idea is not to read the whole table, however. The idea is to use this as a building block for a query, by applying Queryable operators such as {@link org.apache.calcite.linq4j.Queryable#where(org.apache.calcite.linq4j.function.Predicate2)}.
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
            return Pair.of( rep, type.getSqlTypeName().getJdbcOrdinal() );
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
        SqlSelect node = new SqlSelect( SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, physicalTableName, null, null, null, null, null, null, null );
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
        return ResultSetEnumerable.of( jdbcSchema.getConnectionHandler( root ), sql.getSql(), JdbcUtils.ObjectArrayRowBuilder.factory( fieldClasses( typeFactory ) ) );
    }


    @Override
    public Collection getModifiableCollection() {
        throw new RuntimeException( "getModifiableCollection() is not implemented for JDBC adapter!" );
    }


    @Override
    public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        jdbcSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify( cluster, cluster.traitSetOf( Convention.NONE ), table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
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

