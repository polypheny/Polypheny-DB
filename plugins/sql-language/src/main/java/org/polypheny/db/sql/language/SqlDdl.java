/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.util.CoreUtil;


/**
 * Base class for CREATE, DROP and other DDL statements.
 */
public abstract class SqlDdl extends SqlCall {

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


    @NotNull
    protected Optional<? extends LogicalEntity> searchEntity( Context context, SqlIdentifier entityName ) {
        long namespaceId;
        String tableOldName;
        if ( entityName.names.size() == 2 ) { // NamespaceName.EntityName
            namespaceId = context.getSnapshot().getNamespace( entityName.names.get( 0 ) ).orElseThrow().id;
            tableOldName = entityName.names.get( 1 );
        } else { // EntityName
            namespaceId = context.getSnapshot().getNamespace( context.getDefaultNamespaceName() ).orElseThrow().id;
            tableOldName = entityName.names.get( 0 );
        }
        if ( context.getSnapshot().rel().getTable( namespaceId, tableOldName ).isPresent() ) {
            return context.getSnapshot().rel().getTable( namespaceId, tableOldName );
        } else if ( context.getSnapshot().doc().getCollection( namespaceId, tableOldName ).isPresent() ) {
            return context.getSnapshot().doc().getCollection( namespaceId, tableOldName );
        }
        return context.getSnapshot().graph().getGraph( namespaceId );

    }


    /**
     * Returns the table with the given name and the correct data model, or throws if not found.
     */
    @NotNull
    protected LogicalTable getTableFailOnEmpty( Context context, SqlIdentifier tableName ) {
        Optional<? extends LogicalEntity> table = searchEntity( context, tableName );
        if ( table.isEmpty() ) {
            throw new GenericRuntimeException( "Could not find relational entity with name: " + String.join( ".", tableName.names ) );
        }
        if ( table.get().unwrap( LogicalTable.class ).isEmpty() ) {
            throw new GenericRuntimeException( String.format( "Could not find a relational entity with name: %s,but an entity of type: %s",
                    String.join( ".", tableName.names ),
                    table.get().dataModel ) );
        }
        return table.get().unwrap( LogicalTable.class ).get();
    }


    @Nullable
    protected LogicalColumn getColumn( Context context, long tableId, SqlIdentifier columnName ) {
        return context.getSnapshot().rel().getColumn( tableId, columnName.getSimple() ).orElse( null );
    }


    protected List<LogicalColumn> getColumns( Context context, long tableId, SqlNodeList columns ) {
        return columns.getList().stream()
                .map( c -> getColumn( context, tableId, (SqlIdentifier) c ) )
                .collect( Collectors.toList() );
    }


    protected DataStore<?> getDataStoreInstance( SqlIdentifier storeName ) {
        Optional<Adapter<?>> optionalAdapter = AdapterManager.getInstance().getAdapter( storeName.getSimple() );
        if ( optionalAdapter.isEmpty() ) {
            throw CoreUtil.newContextException( storeName.getPos(), RESOURCE.unknownAdapter( storeName.getSimple() ) );
        }
        // Make sure it is a data store instance
        return getDataStore( optionalAdapter.get(), storeName.getPos() );
    }


    protected DataStore<?> getDataStoreInstance( long storeId ) {
        Optional<Adapter<?>> optionalAdapter = AdapterManager.getInstance().getAdapter( storeId );
        if ( optionalAdapter.isEmpty() ) {
            throw new GenericRuntimeException( "Unknown store with id: " + storeId );
        }
        return getDataStore( optionalAdapter.get(), pos );
    }


    @NotNull
    private DataStore<?> getDataStore( Adapter<?> optionalAdapter, ParserPos pos ) {
        // Make sure it is a data store instance
        if ( optionalAdapter instanceof DataStore ) {
            return (DataStore<?>) optionalAdapter;
        } else if ( optionalAdapter instanceof DataSource ) {
            throw CoreUtil.newContextException( pos, RESOURCE.ddlOnDataSource( optionalAdapter.getUniqueName() ) );
        } else {
            throw new GenericRuntimeException( "Unknown kind of adapter: " + optionalAdapter.getClass().getName() );
        }
    }

}
