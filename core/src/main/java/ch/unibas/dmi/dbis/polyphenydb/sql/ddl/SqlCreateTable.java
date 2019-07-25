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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Encoding;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCreate;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.InitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE TABLE", SqlKind.CREATE_TABLE );


    /**
     * Creates a SqlCreateTable.
     */
    SqlCreateTable( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
    }


    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        writer.keyword( "TABLE" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
        if ( columnList != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columnList ) {
                writer.sep( "," );
                c.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        if ( query != null ) {
            writer.keyword( "AS" );
            writer.newlineAndIndent();
            query.unparse( writer, 0, 0 );
        }
    }


    @Override
    public void execute( Context context, CatalogManager catalog ) {
        if ( query != null ) {
            throw new RuntimeException( "Not supported yet" );
        }

        String tableName;
        long schemaId;
        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = catalog.getSchema( context.getTransactionId(), name.names.get( 0 ), name.names.get( 1 ) ).id;
                tableName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = catalog.getSchema( context.getTransactionId(), context.getDatabaseId(), name.names.get( 0 ) ).id;
                tableName = name.names.get( 1 );
            } else { // TableName
                schemaId = catalog.getSchema( context.getTransactionId(), context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableName = name.names.get( 0 );
            }
        } catch ( UnknownDatabaseException | UnknownCollationException | UnknownSchemaTypeException | UnknownEncodingException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownSchemaException e ) {
            if ( ifNotExists ) {
                // It is ok that there is already a table with this name because "IF NOT EXISTS" was specified
                return;
            } else {
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableExists( name.toString() ) );
            }
        }

        try {
            long tableId = catalog.addTable(
                    context.getTransactionId(),
                    tableName,
                    schemaId,
                    context.getCurrentUserId(),
                    Encoding.UTF8,
                    Collation.CASE_INSENSITIVE,
                    TableType.TABLE,
                    null );

            catalog.addDataPlacement( context.getTransactionId(), context.getDefaultStore(), tableId );

            if ( this.columnList == null ) {
                // "CREATE TABLE t" is invalid; because there is no "AS query" we need a list of column names and types, "CREATE TABLE t (INT c)".
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.createTableRequiresColumnList() );
            }

            List<SqlNode> columnList = this.columnList.getList();
            int position = 1;
            for ( Ord<SqlNode> c : Ord.zip( columnList ) ) {
                if ( c.e instanceof SqlColumnDeclaration ) {
                    final SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) c.e;
                    catalog.addColumn(
                            context.getTransactionId(),
                            columnDeclaration.name.getSimple(),
                            tableId,
                            position++,
                            PolySqlType.getPolySqlTypeFromSting( columnDeclaration.dataType.getTypeName().getSimple() ),
                            columnDeclaration.dataType.getScale() == -1 ? null : columnDeclaration.dataType.getScale(),
                            columnDeclaration.dataType.getPrecision() == -1 ? null : columnDeclaration.dataType.getPrecision(),
                            columnDeclaration.dataType.getNullable(),
                            Encoding.UTF8,
                            Collation.CASE_INSENSITIVE,
                            false
                    );
                } else {
                    throw new AssertionError( c.e.getClass() );
                }
            }

            CatalogCombinedTable combinedTable = catalog.getCombinedTable( context.getTransactionId(), tableId );
            StoreManager.getInstance().getStore( context.getDefaultStore() ).createTable( context, combinedTable );
        } catch ( GenericCatalogException | UnknownTableException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Abstract base class for implementations of {@link ModifiableTable}.
     */
    abstract static class AbstractModifiableTable extends AbstractTable implements ModifiableTable {

        AbstractModifiableTable( String tableName ) {
            super();
        }


        public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList,
                boolean flattened ) {
            return LogicalTableModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
        }
    }


    /**
     * Table backed by a Java list.
     */
    static class MutableArrayTable extends AbstractModifiableTable implements Wrapper {

        final List rows = new ArrayList();
        private final RelProtoDataType protoStoredRowType;
        private final RelProtoDataType protoRowType;
        private final InitializerExpressionFactory initializerExpressionFactory;


        /**
         * Creates a MutableArrayTable.
         *
         * @param name Name of table within its schema
         * @param protoStoredRowType Prototype of row type of stored columns (all columns except virtual columns)
         * @param protoRowType Prototype of row type (all columns)
         * @param initializerExpressionFactory How columns are populated
         */
        MutableArrayTable( String name, RelProtoDataType protoStoredRowType, RelProtoDataType protoRowType, InitializerExpressionFactory initializerExpressionFactory ) {
            super( name );
            this.protoStoredRowType = Objects.requireNonNull( protoStoredRowType );
            this.protoRowType = Objects.requireNonNull( protoRowType );
            this.initializerExpressionFactory = Objects.requireNonNull( initializerExpressionFactory );
        }


        public Collection getModifiableCollection() {
            return rows;
        }


        public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
            return new AbstractTableQueryable<T>( queryProvider, schema, this, tableName ) {
                public Enumerator<T> enumerator() {
                    //noinspection unchecked
                    return (Enumerator<T>) Linq4j.enumerator( rows );
                }
            };
        }


        public Type getElementType() {
            return Object[].class;
        }


        public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
            return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
        }


        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
            return protoRowType.apply( typeFactory );
        }


        @Override
        public <C> C unwrap( Class<C> aClass ) {
            if ( aClass.isInstance( initializerExpressionFactory ) ) {
                return aClass.cast( initializerExpressionFactory );
            }
            return super.unwrap( aClass );
        }
    }


    /*
    public void execute( Context context ) {
        final Pair<PolyphenyDbSchema, String> pair = SqlDdlNodes.schema( context, true, name );
        final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final RelDataType queryRowType;
        if ( query != null ) {
            // A bit of a hack: pretend it's a view, to get its row type
            final String sql = query.toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql();
            final ViewTableMacro viewTableMacro = ViewTable.viewMacro( pair.left.plus(), sql, pair.left.path( null ), context.getObjectPath(), false );
            final TranslatableTable x = viewTableMacro.apply( ImmutableList.of() );
            queryRowType = x.getRowType( typeFactory );

            if ( columnList != null && queryRowType.getFieldCount() != columnList.size() ) {
                throw SqlUtil.newContextException( columnList.getParserPosition(), RESOURCE.columnCountMismatch() );
            }
        } else {
            queryRowType = null;
        }
        final List<SqlNode> columnList;
        if ( this.columnList != null ) {
            columnList = this.columnList.getList();
        } else {
            if ( queryRowType == null ) {
                // "CREATE TABLE t" is invalid; because there is no "AS query" we need a list of column names and types, "CREATE TABLE t (INT c)".
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.createTableRequiresColumnList() );
            }
            columnList = new ArrayList<>();
            for ( String name : queryRowType.getFieldNames() ) {
                columnList.add( new SqlIdentifier( name, SqlParserPos.ZERO ) );
            }
        }
        final ImmutableList.Builder<ColumnDef> b = ImmutableList.builder();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
        for ( Ord<SqlNode> c : Ord.zip( columnList ) ) {
            if ( c.e instanceof SqlColumnDeclaration ) {
                final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
                RelDataType type = d.dataType.deriveType( typeFactory, true );
                final Pair<PolyphenyDbSchema, String> pairForType = SqlDdlNodes.schema( context, true, d.dataType.getTypeName() );
                if ( type == null ) {
                    PolyphenyDbSchema.TypeEntry typeEntry = pairForType.left.getType( pairForType.right, false );
                    if ( typeEntry != null ) {
                        type = typeEntry.getType().apply( typeFactory );
                    }
                }
                builder.add( d.name.getSimple(), type );
                if ( d.strategy != ColumnStrategy.VIRTUAL ) {
                    storedBuilder.add( d.name.getSimple(), type );
                }
                b.add( ColumnDef.of( d.expression, type, d.strategy ) );
            } else if ( c.e instanceof SqlIdentifier ) {
                final SqlIdentifier id = (SqlIdentifier) c.e;
                if ( queryRowType == null ) {
                    throw SqlUtil.newContextException( id.getParserPosition(), RESOURCE.createTableRequiresColumnTypes( id.getSimple() ) );
                }
                final RelDataTypeField f = queryRowType.getFieldList().get( c.i );
                final ColumnStrategy strategy =
                        f.getType().isNullable()
                                ? ColumnStrategy.NULLABLE
                                : ColumnStrategy.NOT_NULLABLE;
                b.add( ColumnDef.of( c.e, f.getType(), strategy ) );
                builder.add( id.getSimple(), f.getType() );
                storedBuilder.add( id.getSimple(), f.getType() );
            } else {
                throw new AssertionError( c.e.getClass() );
            }
        }
        final RelDataType rowType = builder.build();
        final RelDataType storedRowType = storedBuilder.build();
        final List<ColumnDef> columns = b.build();
        final InitializerExpressionFactory ief =
                new NullInitializerExpressionFactory() {
                    @Override
                    public ColumnStrategy generationStrategy( RelOptTable table, int iColumn ) {
                        return columns.get( iColumn ).strategy;
                    }


                    @Override
                    public RexNode newColumnDefaultValue( RelOptTable table, int iColumn, InitializerContext context ) {
                        final ColumnDef c = columns.get( iColumn );
                        if ( c.expr != null ) {
                            return context.convertExpression( c.expr );
                        }
                        return super.newColumnDefaultValue( table, iColumn, context );
                    }
                };
        if ( pair.left.plus().getTable( pair.right ) != null ) {
            // Table exists.
            if ( !ifNotExists ) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableExists( pair.right ) );
            }
            return;
        }
        // Table does not exist. Create it.
        pair.left.add( pair.right,
                new MutableArrayTable(
                        pair.right,
                        RelDataTypeImpl.proto( storedRowType ),
                        RelDataTypeImpl.proto( rowType ), ief ) );
        if ( query != null ) {
            SqlDdlNodes.populate( name, query, context );
        }
    }

    // Column Definition
    private static class ColumnDef {

        final SqlNode expr;
        final RelDataType type;
        final ColumnStrategy strategy;


        private ColumnDef( SqlNode expr, RelDataType type, ColumnStrategy strategy ) {
            this.expr = expr;
            this.type = type;
            this.strategy = Objects.requireNonNull( strategy );
            Preconditions.checkArgument(
                    strategy == ColumnStrategy.NULLABLE
                            || strategy == ColumnStrategy.NOT_NULLABLE
                            || expr != null );
        }


        static ColumnDef of( SqlNode expr, RelDataType type, ColumnStrategy strategy ) {
            return new ColumnDef( expr, type, strategy );
        }
    }

    */
}

