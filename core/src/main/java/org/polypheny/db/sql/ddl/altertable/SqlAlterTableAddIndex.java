/*
 * Copyright 2019-2021 The Polypheny Project
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
 */

package org.polypheny.db.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.AvailableIndexMethod;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD INDEX} statement.
 */
public class SqlAlterTableAddIndex extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier indexName;
    private final SqlIdentifier indexMethod;
    private final SqlNodeList columnList;
    private final boolean unique;
    private final SqlIdentifier storeName;


    public SqlAlterTableAddIndex(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnList,
            boolean unique,
            SqlIdentifier indexMethod,
            SqlIdentifier indexName,
            SqlIdentifier storeName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.unique = unique;
        this.indexName = indexName;
        this.indexMethod = indexMethod;
        this.storeName = storeName;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName, indexMethod, indexName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        if ( unique ) {
            writer.keyword( "UNIQUE" );
        }
        writer.keyword( "INDEX" );
        indexName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        columnList.unparse( writer, leftPrec, rightPrec );
        if ( indexMethod != null ) {
            writer.keyword( "USING" );
            indexMethod.unparse( writer, leftPrec, rightPrec );
        }
        if ( storeName != null ) {
            writer.keyword( "ON" );
            writer.keyword( "STORE" );
            storeName.unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        try {
            List<Long> columnIds = new LinkedList<>();
            for ( SqlNode node : columnList.getList() ) {
                String columnName = node.toString();
                CatalogColumn catalogColumn = Catalog.getInstance().getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }

            IndexType type = IndexType.MANUAL;

            // Make sure that this is a table of type TABLE (and not SOURCE)
            if ( catalogTable.tableType != TableType.TABLE ) {
                throw SqlUtil.newContextException( table.getParserPosition(), RESOURCE.ddlOnSourceTable() );
            }

            // Check if there is already an index with this name for this table
            if ( Catalog.getInstance().checkIfExistsIndex( catalogTable.id, indexName.getSimple() ) ) {
                throw SqlUtil.newContextException( indexName.getParserPosition(), RESOURCE.indexExists( indexName.getSimple() ) );
            }

            if ( storeName == null ) { // Polystore Index
                String method;
                String methodDisplayName;
                if ( indexMethod != null ) {
                    AvailableIndexMethod aim = null;
                    for ( AvailableIndexMethod availableIndexMethod : IndexManager.getAvailableIndexMethods() ) {
                        if ( availableIndexMethod.name.equals( indexMethod.getSimple() ) ) {
                            aim = availableIndexMethod;
                        }
                    }
                    if ( aim == null ) {
                        throw SqlUtil.newContextException(
                                indexMethod.getParserPosition(),
                                RESOURCE.unknownIndexMethod( indexMethod.getSimple() ) );
                    }
                    method = aim.name;
                    methodDisplayName = aim.displayName;
                } else {
                    method = IndexManager.getDefaultIndexMethod().name;
                    methodDisplayName = IndexManager.getDefaultIndexMethod().displayName;
                }

                long indexId = Catalog.getInstance().addIndex(
                        catalogTable.id,
                        columnIds,
                        unique,
                        method,
                        methodDisplayName,
                        0,
                        type,
                        indexName.getSimple() );

                IndexManager.getInstance().addIndex( Catalog.getInstance().getIndex( indexId ), statement );
            } else { // Store Index
                DataStore storeInstance = getDataStoreInstance( storeName );
                if ( storeInstance == null ) {
                    throw SqlUtil.newContextException(
                            storeName.getParserPosition(),
                            RESOURCE.unknownAdapter( storeName.getSimple() ) );
                }

                // Check if there if all required columns are present on this store
                for ( long columnId : columnIds ) {
                    if ( !Catalog.getInstance().checkIfExistsColumnPlacement( storeInstance.getAdapterId(), columnId ) ) {
                        throw SqlUtil.newContextException(
                                storeName.getParserPosition(),
                                RESOURCE.missingColumnPlacement( Catalog.getInstance().getColumn( columnId ).name, storeInstance.getUniqueName() ) );
                    }
                }

                String method;
                String methodDisplayName;
                if ( indexMethod != null ) {
                    AvailableIndexMethod aim = null;
                    for ( AvailableIndexMethod availableIndexMethod : storeInstance.getAvailableIndexMethods() ) {
                        if ( availableIndexMethod.name.equals( indexMethod.getSimple() ) ) {
                            aim = availableIndexMethod;
                        }
                    }
                    if ( aim == null ) {
                        throw SqlUtil.newContextException(
                                indexMethod.getParserPosition(),
                                RESOURCE.unknownIndexMethod( indexMethod.getSimple() ) );
                    }
                    method = aim.name;
                    methodDisplayName = aim.displayName;
                } else {
                    method = storeInstance.getDefaultIndexMethod().name;
                    methodDisplayName = storeInstance.getDefaultIndexMethod().displayName;
                }

                long indexId = Catalog.getInstance().addIndex(
                        catalogTable.id,
                        columnIds,
                        unique,
                        method,
                        methodDisplayName,
                        storeInstance.getAdapterId(),
                        type,
                        indexName.getSimple() );

                storeInstance.addIndex( context, Catalog.getInstance().getIndex( indexId ) );
            }
        } catch ( UnknownColumnException e ) {
            throw SqlUtil.newContextException( columnList.getParserPosition(), RESOURCE.columnNotFound( e.getColumnName() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( table.getParserPosition(), RESOURCE.schemaNotFound( e.getSchemaName() ) );
        } catch ( UnknownTableException e ) {
            throw SqlUtil.newContextException( table.getParserPosition(), RESOURCE.tableNotFound( e.getTableName() ) );
        } catch ( GenericCatalogException | UnknownKeyException | UnknownUserException | UnknownDatabaseException | TransactionException e ) {
            throw new RuntimeException( e );
        }
    }

}

