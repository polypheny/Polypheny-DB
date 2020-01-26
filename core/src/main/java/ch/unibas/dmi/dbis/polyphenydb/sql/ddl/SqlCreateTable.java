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

import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.PlacementType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.NameGenerator;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private final SqlIdentifier store;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE TABLE", SqlKind.CREATE_TABLE );


    /**
     * Creates a SqlCreateTable.
     */
    SqlCreateTable( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query, SqlIdentifier store ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
        this.store = store; // ON STORE [store name]; may be null
    }


    @Override
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
        if ( store != null ) {
            writer.keyword( "ON STORE" );
            store.unparse( writer, 0, 0 );
        }
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        if ( query != null ) {
            throw new RuntimeException( "Not supported yet" );
        }

        String tableName;
        long schemaId;
        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                tableName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), name.names.get( 0 ) ).id;
                tableName = name.names.get( 1 );
            } else { // TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableName = name.names.get( 0 );
            }
        } catch ( UnknownDatabaseException | UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownSchemaException e ) {
            if ( ifNotExists ) {
                // It is ok that there is already a table with this name because "IF NOT EXISTS" was specified
                return;
            } else {
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.schemaNotFound( name.toString() ) );
            }
        }

        try {
            // Check if there is already a table with this name
            if ( transaction.getCatalog().checkIfExistsTable( schemaId, tableName ) ) {
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableExists( tableName ) );
            }

            if ( this.columnList == null ) {
                // "CREATE TABLE t" is invalid; because there is no "AS query" we need a list of column names and types, "CREATE TABLE t (INT c)".
                throw SqlUtil.newContextException( columnList.getParserPosition(), RESOURCE.createTableRequiresColumnList() );
            }

            int storeId = context.getDefaultStore();
            if ( this.store != null ) {
                Store storeInstance = StoreManager.getInstance().getStore( this.store.getSimple() );
                if ( storeInstance == null ) {
                    throw SqlUtil.newContextException( store.getParserPosition(), RESOURCE.unknownStoreName( store.getSimple() ) );
                }
                storeId = storeInstance.getStoreId();
            }

            long tableId = transaction.getCatalog().addTable(
                    tableName,
                    schemaId,
                    context.getCurrentUserId(),
                    TableType.TABLE,
                    null );

            transaction.getCatalog().addDataPlacement( storeId, tableId, PlacementType.AUTOMATIC );

            List<SqlNode> columnList = this.columnList.getList();
            int position = 1;
            for ( Ord<SqlNode> c : Ord.zip( columnList ) ) {
                if ( c.e instanceof SqlColumnDeclaration ) {
                    final SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) c.e;
                    final PolySqlType polySqlType = PolySqlType.getPolySqlTypeFromSting( columnDeclaration.dataType.getTypeName().getSimple() );
                    Collation collation = null;
                    if ( polySqlType.isCharType() ) {
                        if ( columnDeclaration.collation != null ) {
                            collation = Collation.parse( columnDeclaration.collation );
                        } else {
                            collation = Collation.getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() ); // Set default collation
                        }
                    }
                    long addedColumnId = transaction.getCatalog().addColumn(
                            columnDeclaration.name.getSimple(),
                            tableId,
                            position++,
                            polySqlType,
                            columnDeclaration.dataType.getPrecision() == -1 ? null : columnDeclaration.dataType.getPrecision(),
                            columnDeclaration.dataType.getScale() == -1 ? null : columnDeclaration.dataType.getScale(),
                            columnDeclaration.dataType.getNullable(),
                            collation
                    );

                    // Add default value
                    if ( ((SqlColumnDeclaration) c.e).expression != null ) {
                        // TODO: String is only a temporal solution for default values
                        String v = ((SqlColumnDeclaration) c.e).expression.toString();
                        if ( v.startsWith( "'" ) ) {
                            v = v.substring( 1, v.length() - 1 );
                        }
                        transaction.getCatalog().setDefaultValue( addedColumnId, PolySqlType.VARCHAR, v );
                    }
                } else if ( c.e instanceof SqlKeyConstraint ) {
                    SqlKeyConstraint constraint = (SqlKeyConstraint) c.e;
                    List<Long> columnIds = new LinkedList<>();
                    for ( SqlNode node : constraint.getColumnList().getList() ) {
                        String columnName = node.toString();
                        CatalogColumn catalogColumn = transaction.getCatalog().getColumn( tableId, columnName );
                        columnIds.add( catalogColumn.id );
                    }
                    if ( constraint.getOperator() == SqlKeyConstraint.PRIMARY ) {
                        transaction.getCatalog().addPrimaryKey( tableId, columnIds );
                    } else if ( constraint.getOperator() == SqlKeyConstraint.UNIQUE ) {
                        String constraintName;
                        if ( constraint.getName() == null ) {
                            constraintName = NameGenerator.generateConstraintName();
                        } else {
                            constraintName = constraint.getName().getSimple();
                        }
                        transaction.getCatalog().addUniqueConstraint( tableId, constraintName, columnIds );
                    }
                } else {
                    throw new AssertionError( c.e.getClass() );
                }
            }

            CatalogCombinedTable combinedTable = transaction.getCatalog().getCombinedTable( tableId );
            StoreManager.getInstance().getStore( storeId ).createTable( context, combinedTable );
        } catch ( GenericCatalogException | UnknownTableException | UnknownColumnException | UnknownCollationException e ) {
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


        @Override
        public TableModify toModificationRel(
                RelOptCluster cluster,
                RelOptTable table,
                Prepare.CatalogReader catalogReader,
                RelNode child,
                TableModify.Operation operation,
                List<String> updateColumnList,
                List<RexNode> sourceExpressionList,
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


        @Override
        public Collection getModifiableCollection() {
            return rows;
        }


        @Override
        public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
            return new AbstractTableQueryable<T>( dataContext, schema, this, tableName ) {
                @Override
                public Enumerator<T> enumerator() {
                    //noinspection unchecked
                    return (Enumerator<T>) Linq4j.enumerator( rows );
                }
            };
        }


        @Override
        public Type getElementType() {
            return Object[].class;
        }


        @Override
        public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
            return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
        }


        @Override
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

}

