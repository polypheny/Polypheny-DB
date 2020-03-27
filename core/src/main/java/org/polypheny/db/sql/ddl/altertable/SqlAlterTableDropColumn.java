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
 */

package org.polypheny.db.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.CatalogManager;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedKey;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name DROP COLUMN name} statement.
 */
public class SqlAlterTableDropColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier column;


    public SqlAlterTableDropColumn( SqlParserPos pos, SqlIdentifier table, SqlIdentifier column ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.column = Objects.requireNonNull( column );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, column );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "DROP" );
        writer.keyword( "COLUMN" );
        column.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogTable catalogTable = getCatalogTable( context, table );

        if ( catalogTable.columnIds.size() < 2 ) {
            throw new RuntimeException( "Cannot drop sole column of table " + catalogTable.name );
        }

        if ( column.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + column.toString() );
        }

        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, column );
        try {
            // Check whether all stores support schema changes
            for ( CatalogColumnPlacement dp : CatalogManager.getInstance().getCatalog().getColumnPlacements( catalogColumn.id ) ) {
                if ( StoreManager.getInstance().getStore( dp.storeId ).isSchemaReadOnly() ) {
                    throw SqlUtil.newContextException(
                            SqlParserPos.ZERO,
                            RESOURCE.storeIsSchemaReadOnly( StoreManager.getInstance().getStore( dp.storeId ).getUniqueName() ) );
                }
            }

            // Check if column is part of an key
            for ( CatalogKey key : CatalogManager.getInstance().getCatalog().getTableKeys( catalogTable.id ) ) {
                if ( key.columnIds.contains( catalogColumn.id ) ) {
                    CatalogCombinedKey combinedKey = CatalogManager.getInstance().getCatalog().getCombinedKey( key.id );
                    if ( combinedKey.isPrimaryKey() ) {
                        throw new PolyphenyDbException( "Cannot drop column '" + catalogColumn.name + "' because it is part of the primary key." );
                    } else if ( combinedKey.getIndexes().size() > 0 ) {
                        throw new PolyphenyDbException( "Cannot drop column '" + catalogColumn.name + "' because it is part of the index with the name: '" + combinedKey.getIndexes().get( 0 ).name + "'." );
                    } else if ( combinedKey.getForeignKeys().size() > 0 ) {
                        throw new PolyphenyDbException( "Cannot drop column '" + catalogColumn.name + "' because it is part of the foreign key with the name: '" + combinedKey.getForeignKeys().get( 0 ).name + "'." );
                    } else if ( combinedKey.getConstraints().size() > 0 ) {
                        throw new PolyphenyDbException( "Cannot drop column '" + catalogColumn.name + "' because it is part of the constraint with the name: '" + combinedKey.getConstraints().get( 0 ).name + "'." );
                    }
                    throw new PolyphenyDbException( "Ok, strange... Something is going wrong here!" );
                }
            }

            // Delete column from underlying data stores
            for ( CatalogColumnPlacement dp : CatalogManager.getInstance().getCatalog().getColumnPlacementsByColumn( catalogColumn.id ) ) {
                StoreManager.getInstance().getStore( dp.storeId ).dropColumn( context, dp );
                CatalogManager.getInstance().getCatalog().deleteColumnPlacement( dp.storeId, dp.columnId );
            }

            // Delete from catalog
            List<CatalogColumn> columns = CatalogManager.getInstance().getCatalog().getColumns( catalogTable.id );
            CatalogManager.getInstance().getCatalog().deleteColumn( catalogColumn.id );
            if ( catalogColumn.position != columns.size() ) {
                // Update position of the other columns
                for ( int i = catalogColumn.position; i < columns.size(); i++ ) {
                    CatalogManager.getInstance().getCatalog().setColumnPosition( columns.get( i ).id, i );
                }
            }

        } catch ( GenericCatalogException | UnknownKeyException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }

}

