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


import java.util.LinkedList;
import java.util.List;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;


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
    public void execute( Context context, Transaction transaction ) {
        // Get table
        final CatalogCombinedTable table;
        try {
            table = getCatalogCombinedTable( context, transaction, name );
        } catch ( PolyphenyDbContextException e ) {
            if ( ifExists ) {
                // It is ok that there is no database / schema / table with this name because "IF EXISTS" was specified
                return;
            } else {
                throw e;
            }
        }

        // Check if there are foreign keys referencing this table
        List<CatalogForeignKey> selfRefsToDelete = new LinkedList<>();
        try {
            List<CatalogForeignKey> exportedKeys = transaction.getCatalog().getExportedKeys( table.getTable().id );
            if ( exportedKeys.size() > 0 ) {
                for ( CatalogForeignKey foreignKey : exportedKeys ) {
                    if ( foreignKey.tableId == table.getTable().id ) {
                        // If this is a self-reference, drop it later.
                        selfRefsToDelete.add( foreignKey );
                    } else {
                        throw new PolyphenyDbException( "Cannot drop table '" + table.getSchema().name + "." + table.getTable().name + "' because it is being referenced by '" + exportedKeys.get( 0 ).schemaName + "." + exportedKeys.get( 0 ).tableName + "'." );
                    }
                }

            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while retrieving list of exported keys.", e );
        }

        // Delete data from the stores and remove the column placement
        try {
            for ( int storeId : table.getColumnPlacementsByStore().keySet() ) {
                StoreManager.getInstance().getStore( storeId ).dropTable( context, table );
                // Delete column placement in catalog
                for ( CatalogColumn catalogColumn : table.getColumns() ) {
                    transaction.getCatalog().deleteColumnPlacement( storeId, catalogColumn.id );
                }
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while deleting data from stores.", e );
        }

        // Delete the self-referencing foreign keys
        try {
            for ( CatalogForeignKey foreignKey : selfRefsToDelete ) {
                transaction.getCatalog().deleteForeignKey( foreignKey.id );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while deleting self-referencing foreign key constraints.", e );
        }

        // Delete indexes of this table
        try {
            List<CatalogIndex> indexes = transaction.getCatalog().getIndexes( table.getTable().id, false );
            for ( CatalogIndex index : indexes ) {
                transaction.getCatalog().deleteIndex( index.id );
            }
        } catch ( GenericCatalogException | UnknownIndexException e ) {
            throw new PolyphenyDbContextException( "Exception while dropping indexes.", e );
        }

        // Delete keys and constraints
        try {
            // Remove primary key
            transaction.getCatalog().deletePrimaryKey( table.getTable().id );
            // Delete all foreign keys of the table
            List<CatalogForeignKey> foreignKeys = transaction.getCatalog().getForeignKeys( table.getTable().id );
            for ( CatalogForeignKey foreignKey : foreignKeys ) {
                transaction.getCatalog().deleteForeignKey( foreignKey.id );
            }
            // Delete all constraints of the table
            for ( CatalogConstraint constraint : transaction.getCatalog().getConstraints( table.getTable().id ) ) {
                transaction.getCatalog().deleteConstraint( constraint.id );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while dropping keys.", e );
        }

        // Delete columns
        try {
            for ( CatalogColumn catalogColumn : table.getColumns() ) {
                transaction.getCatalog().deleteColumn( catalogColumn.id );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while dropping columns.", e );
        }

        // Delete the table
        try {
            transaction.getCatalog().deleteTable( table.getTable().id );
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while dropping the table.", e );
        }
    }
}

