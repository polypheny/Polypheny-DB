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

import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDataPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogIndex;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownIndexException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownKeyException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbContextException;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import java.util.List;


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
        String tableName;
        long schemaId;
        final CatalogCombinedTable table;
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
            table = transaction.getCatalog().getCombinedTable( transaction.getCatalog().getTable( schemaId, tableName ).id );
        } catch ( UnknownDatabaseException | UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownSchemaException e ) {
            if ( ifExists ) {
                // It is ok that there is no schema with this name because "IF EXISTS" was specified
                return;
            } else {
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.schemaNotFound( name.toString() ) );
            }
        } catch ( UnknownTableException e ) {
            if ( ifExists ) {
                // It is ok that there is no table with this name because "IF EXISTS" was specified
                return;
            } else {
                throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableNotFound( name.toString() ) );
            }
        }

        // Check if there are foreign keys referencing this table
        try {
            List<CatalogForeignKey> exportedKeys = transaction.getCatalog().getExportedKeys( table.getTable().id );
            if ( exportedKeys.size() > 0 ) {
                throw new PolyphenyDbException( "Cannot drop table '" + table.getSchema().name + "." + tableName + "' because it is being referenced by '" + exportedKeys.get( 0 ).schemaName + "." + exportedKeys.get( 0 ).tableName + "'." );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while retrieving list of exported keys.", e );
        }

        // Delete data from the stores and remove the data placement
        try {
            for ( CatalogDataPlacement dp : table.getPlacements() ) {
                StoreManager.getInstance().getStore( dp.storeId ).dropTable( table );
                // Delete data placement in catalog
                transaction.getCatalog().deleteDataPlacement( dp.storeId, dp.tableId );
            }
        } catch ( GenericCatalogException e ) {
            throw new PolyphenyDbContextException( "Exception while deleting data from stores.", e );
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

        // Delete keys
        try {
            // Remove primary key
            transaction.getCatalog().deletePrimaryKey( table.getTable().id );
            // Delete all foreign keys of the table
            List<CatalogForeignKey> foreignKeys = transaction.getCatalog().getForeignKeys( table.getTable().id );
            for ( CatalogForeignKey foreignKey : foreignKeys ) {
                transaction.getCatalog().deleteConstraint( table.getTable().id, foreignKey.name );
            }
            // Delete all remaining keys (unique keys and the primary key) of the table
            for ( CatalogKey key : transaction.getCatalog().getKeys( table.getTable().id ) ) {
                transaction.getCatalog().deleteKey( key.id );
            }
        } catch ( GenericCatalogException | UnknownKeyException e ) {
            throw new PolyphenyDbContextException( "Exception while dropping keys.", e );
        }

        // Delete columns
        try {
            for ( CatalogColumn catalogColumn : table.getColumns() ) {
                // delete default values
                transaction.getCatalog().deleteDefaultValue( catalogColumn.id );
                // delete the column itself
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

