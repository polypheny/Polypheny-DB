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

package org.polypheny.db.sql.ddl;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
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
import org.polypheny.db.PolySqlType;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.NameGenerator;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.sql.SqlCreate;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql2rel.InitializerExpressionFactory;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;


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
                throw SqlUtil.newContextException( SqlParserPos.ZERO, RESOURCE.createTableRequiresColumnList() );
            }

            List<Store> stores;
            if ( this.store != null ) {
                Store storeInstance = StoreManager.getInstance().getStore( this.store.getSimple() );
                if ( storeInstance == null ) {
                    throw SqlUtil.newContextException( store.getParserPosition(), RESOURCE.unknownStoreName( store.getSimple() ) );
                }
                stores = ImmutableList.of( storeInstance );
            } else {
                // TODO: Ask router on which store(s) the table should be placed
                stores = transaction.getRouter().createTable( schemaId, transaction );
            }

            long tableId = transaction.getCatalog().addTable(
                    tableName,
                    schemaId,
                    context.getCurrentUserId(),
                    TableType.TABLE,
                    null );

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

                    for ( Store s : stores ) {
                        transaction.getCatalog().addColumnPlacement(
                                s.getStoreId(),
                                addedColumnId,
                                store == null ? PlacementType.AUTOMATIC : PlacementType.MANUAL,
                                null,
                                null,
                                null );
                    }

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
            for ( Store store : stores ) {
                store.createTable( context, combinedTable );
            }
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

