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

package org.polypheny.db.sql;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.Objects;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.parser.SqlParserPos;


/**
 * Base class for CREATE, DROP and other DDL statements.
 */
public abstract class SqlDdl extends SqlCall {

    /**
     * Use this operator only if you don't have a better one.
     */
    protected static final SqlOperator DDL_OPERATOR = new SqlSpecialOperator( "DDL", SqlKind.OTHER_DDL );

    private final SqlOperator operator;


    /**
     * Creates a SqlDdl.
     */
    public SqlDdl( SqlOperator operator, SqlParserPos pos ) {
        super( pos );
        this.operator = Objects.requireNonNull( operator );
    }


    @Override
    public SqlOperator getOperator() {
        return operator;
    }


    protected CatalogTable getCatalogTable( Context context, SqlIdentifier tableName ) {
        CatalogTable catalogTable;
        try {
            long schemaId;
            String tableOldName;
            Catalog catalog = Catalog.getInstance();
            if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = catalog.getSchema( tableName.names.get( 0 ), tableName.names.get( 1 ) ).id;
                tableOldName = tableName.names.get( 2 );
            } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), tableName.names.get( 0 ) ).id;
                tableOldName = tableName.names.get( 1 );
            } else { // TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableOldName = tableName.names.get( 0 );
            }
            catalogTable = catalog.getTable( schemaId, tableOldName );
        } catch ( UnknownDatabaseException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.databaseNotFound( tableName.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.schemaNotFound( tableName.toString() ) );
        } catch ( UnknownTableException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.tableNotFound( tableName.toString() ) );
        } catch ( UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
        return catalogTable;
    }


    protected CatalogColumn getCatalogColumn( long tableId, SqlIdentifier columnName ) {
        CatalogColumn catalogColumn;
        try {
            catalogColumn = Catalog.getInstance().getColumn( tableId, columnName.getSimple() );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownColumnException e ) {
            throw SqlUtil.newContextException( columnName.getParserPosition(), RESOURCE.columnNotFoundInTable( columnName.getSimple(), tableId + "" ) );
        }
        return catalogColumn;
    }


    protected DataStore getDataStoreInstance( SqlIdentifier storeName ) {
        Adapter adapterInstance = StoreManager.getInstance().getAdapter( storeName.getSimple() );
        if ( adapterInstance == null ) {
            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        // Make sure it is a data store instance
        if ( adapterInstance instanceof DataStore ) {
            return (DataStore) adapterInstance;
        } else if ( adapterInstance instanceof DataSource ) {
            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.ddlOnDataSource( adapterInstance.getUniqueName() ) );
        } else {
            throw new RuntimeException( "Unknown kind of adapter: " + adapterInstance.getClass().getName() );
        }
    }


    protected DataStore getDataStoreInstance( int storeId ) {
        Adapter adapterInstance = StoreManager.getInstance().getAdapter( storeId );
        if ( adapterInstance == null ) {
            throw new RuntimeException( "Unknown store id: " + storeId );
        }
        // Make sure it is a data store instance
        if ( adapterInstance instanceof DataStore ) {
            return (DataStore) adapterInstance;
        } else if ( adapterInstance instanceof DataSource ) {
            throw SqlUtil.newContextException( pos, RESOURCE.ddlOnDataSource( adapterInstance.getUniqueName() ) );
        } else {
            throw new RuntimeException( "Unknown kind of adapter: " + adapterInstance.getClass().getName() );
        }
    }

}
