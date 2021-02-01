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

import java.util.LinkedList;
import java.util.List;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;


/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTable extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP TABLE", SqlKind.DROP_TABLE );


    /**
     * Creates a SqlDropTable.
     */
    SqlDropTable( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        // Get table
        final CatalogTable table;
        Catalog catalog = Catalog.getInstance();
        try {
            table = getCatalogTable( context, name );
        } catch ( PolyphenyDbContextException e ) {
            if ( ifExists ) {
                // It is ok that there is no database / schema / table with this name because "IF EXISTS" was specified
                return;
            } else {
                throw e;
            }
        }

        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( table.tableType != TableType.TABLE ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.ddlOnSourceTable() );
        }

        // Check if there are foreign keys referencing this table
        List<CatalogForeignKey> selfRefsToDelete = new LinkedList<>();
        List<CatalogForeignKey> exportedKeys = catalog.getExportedKeys( table.id );
        if ( exportedKeys.size() > 0 ) {
            for ( CatalogForeignKey foreignKey : exportedKeys ) {
                if ( foreignKey.tableId == table.id ) {
                    // If this is a self-reference, drop it later.
                    selfRefsToDelete.add( foreignKey );
                } else {
                    throw new PolyphenyDbException( "Cannot drop table '" + table.getSchemaName() + "." + table.name + "' because it is being referenced by '" + exportedKeys.get( 0 ).getSchemaName() + "." + exportedKeys.get( 0 ).getTableName() + "'." );
                }
            }

        }

        // Make sure that all adapters are of type store (and not source)
        for ( int storeId : table.placementsByAdapter.keySet() ) {
            getDataStoreInstance( storeId );
        }

        // Delete all indexes
        for ( CatalogIndex index : catalog.getIndexes( table.id, false ) ) {
            if ( index.location == 0 ) {
                // Delete polystore index
                IndexManager.getInstance().deleteIndex( index );
            } else {
                // Delete index on store
                AdapterManager.getInstance().getStore( index.location ).dropIndex( context, index );
            }
            // Delete index in catalog
            catalog.deleteIndex( index.id );
        }

        // Delete data from the stores and remove the column placement
        for ( int storeId : table.placementsByAdapter.keySet() ) {
            // Delete table on store
            AdapterManager.getInstance().getStore( storeId ).dropTable( context, table );
            // Inform routing
            statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapter( storeId, table.id ) );
            // Delete column placement in catalog
            for ( Long columnId : table.columnIds ) {
                if ( catalog.checkIfExistsColumnPlacement( storeId, columnId ) ) {
                    catalog.deleteColumnPlacement( storeId, columnId );
                }
            }
        }

        // Delete the self-referencing foreign keys
        try {
            for ( CatalogForeignKey foreignKey : selfRefsToDelete ) {
                catalog.deleteForeignKey( foreignKey.id );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while deleting self-referencing foreign key constraints.", e );
        }

        // Delete indexes of this table
        List<CatalogIndex> indexes = catalog.getIndexes( table.id, false );
        for ( CatalogIndex index : indexes ) {
            catalog.deleteIndex( index.id );
            IndexManager.getInstance().deleteIndex( index );
        }

        // Delete keys and constraints
        try {
            // Remove primary key
            catalog.deletePrimaryKey( table.id );
            // Delete all foreign keys of the table
            List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( table.id );
            for ( CatalogForeignKey foreignKey : foreignKeys ) {
                catalog.deleteForeignKey( foreignKey.id );
            }
            // Delete all constraints of the table
            for ( CatalogConstraint constraint : catalog.getConstraints( table.id ) ) {
                catalog.deleteConstraint( constraint.id );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while dropping keys.", e );
        }

        // Delete columns
        for ( Long columnId : table.columnIds ) {
            catalog.deleteColumn( columnId );
        }

        // Delete the table
        catalog.deleteTable( table.id );

        // Rest plan cache and implementation cache
        statement.getQueryProcessor().resetCaches();
    }

}

