/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.Objects;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.util.CoreUtil;


/**
 * Base class for CREATE, DROP and other DDL statements.
 */
public abstract class SqlDdl extends SqlCall {

    /**
     * Use this operator only if you don't have a better one.
     */
    protected static final SqlOperator DDL_OPERATOR = new SqlSpecialOperator( "DDL", Kind.OTHER_DDL );

    private final SqlOperator operator;


    /**
     * Creates a SqlDdl.
     */
    public SqlDdl( SqlOperator operator, ParserPos pos ) {
        super( pos );
        this.operator = Objects.requireNonNull( operator );
    }


    @Override
    public Operator getOperator() {
        return operator;
    }


    protected LogicalTable getFromCatalog( Context context, SqlIdentifier tableName ) {
        long schemaId;
        String tableOldName;
        if ( tableName.names.size() == 3 ) { // DatabaseName.NamespaceName.TableName
            schemaId = context.getSnapshot().getNamespace( tableName.names.get( 1 ) ).id;
            tableOldName = tableName.names.get( 2 );
        } else if ( tableName.names.size() == 2 ) { // NamespaceName.TableName
            schemaId = context.getSnapshot().getNamespace( tableName.names.get( 0 ) ).id;
            tableOldName = tableName.names.get( 1 );
        } else { // TableName
            schemaId = context.getSnapshot().getNamespace( context.getDefaultNamespaceName() ).id;
            tableOldName = tableName.names.get( 0 );
        }
        return context.getSnapshot().rel().getTable( schemaId, tableOldName ).orElseThrow();
    }


    protected LogicalColumn getCatalogColumn( Context context, long tableId, SqlIdentifier columnName ) {
        return context.getSnapshot().rel().getColumn( tableId, columnName.getSimple() ).orElseThrow();
    }


    protected DataStore<?> getDataStoreInstance( SqlIdentifier storeName ) {
        Adapter<?> adapterInstance = AdapterManager.getInstance().getAdapter( storeName.getSimple() );
        if ( adapterInstance == null ) {
            throw CoreUtil.newContextException( storeName.getPos(), RESOURCE.unknownAdapter( storeName.getSimple() ) );
        }
        // Make sure it is a data store instance
        if ( adapterInstance instanceof DataStore ) {
            return (DataStore<?>) adapterInstance;
        } else if ( adapterInstance instanceof DataSource ) {
            throw CoreUtil.newContextException( storeName.getPos(), RESOURCE.ddlOnDataSource( adapterInstance.getUniqueName() ) );
        } else {
            throw new RuntimeException( "Unknown kind of adapter: " + adapterInstance.getClass().getName() );
        }
    }


    protected DataStore<?> getDataStoreInstance( long storeId ) {
        Adapter<?> adapterInstance = AdapterManager.getInstance().getAdapter( storeId );
        if ( adapterInstance == null ) {
            throw new RuntimeException( "Unknown store id: " + storeId );
        }
        // Make sure it is a data store instance
        if ( adapterInstance instanceof DataStore ) {
            return (DataStore<?>) adapterInstance;
        } else if ( adapterInstance instanceof DataSource ) {
            throw CoreUtil.newContextException( pos, RESOURCE.ddlOnDataSource( adapterInstance.getUniqueName() ) );
        } else {
            throw new RuntimeException( "Unknown kind of adapter: " + adapterInstance.getClass().getName() );
        }
    }

}
