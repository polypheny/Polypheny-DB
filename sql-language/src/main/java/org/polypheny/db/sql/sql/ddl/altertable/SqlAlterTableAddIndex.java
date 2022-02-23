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

package org.polypheny.db.sql.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.AlterSourceException;
import org.polypheny.db.ddl.exception.IndexExistsException;
import org.polypheny.db.ddl.exception.MissingColumnPlacementException;
import org.polypheny.db.ddl.exception.UnknownIndexMethodException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlNodeList;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.CoreUtil;
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
            ParserPos pos,
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
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName, indexMethod, indexName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
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
    public void execute( Context context, Statement statement, QueryParameters parameters ) {

        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.tableType != TableType.TABLE && catalogTable.tableType != TableType.MATERIALIZED_VIEW ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE ADD INDEX because " + catalogTable.name + " is not a table or materialized view." );
        }

        DataStore storeInstance = null;
        if ( storeName != null ) {
            storeInstance = getDataStoreInstance( storeName );

            if ( storeInstance == null ) {
                throw CoreUtil.newContextException(
                        storeName.getPos(),
                        RESOURCE.unknownAdapter( storeName.getSimple() ) );
            }
        }

        String indexMethodName = indexMethod != null ? indexMethod.getSimple() : null;

        try {
            DdlManager.getInstance().addIndex(
                    catalogTable,
                    indexMethodName,
                    columnList.getList().stream().map( Node::toString ).collect( Collectors.toList() ),
                    indexName.getSimple(),
                    unique,
                    storeInstance,
                    statement );
        } catch ( UnknownColumnException e ) {
            throw CoreUtil.newContextException( columnList.getPos(), RESOURCE.columnNotFound( e.getColumnName() ) );
        } catch ( UnknownSchemaException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.schemaNotFound( e.getSchemaName() ) );
        } catch ( UnknownTableException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.tableNotFound( e.getTableName() ) );
        } catch ( UnknownIndexMethodException e ) {
            throw CoreUtil.newContextException(
                    indexMethod.getPos(),
                    RESOURCE.unknownIndexMethod( indexMethod.getSimple() ) );
        } catch ( AlterSourceException e ) {
            throw CoreUtil.newContextException( table.getPos(), RESOURCE.ddlOnSourceTable() );
        } catch ( IndexExistsException e ) {
            throw CoreUtil.newContextException( indexName.getPos(), RESOURCE.indexExists( indexName.getSimple() ) );
        } catch ( MissingColumnPlacementException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.missingColumnPlacement( e.getColumnName(), storeInstance.getUniqueName() ) );
        } catch ( GenericCatalogException | UnknownKeyException | UnknownUserException | UnknownDatabaseException | TransactionException e ) {
            throw new RuntimeException( e );
        }
    }

}

