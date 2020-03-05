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


import java.util.List;
import java.util.Objects;
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedKey;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
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
        CatalogCombinedTable catalogTable = getCatalogCombinedTable( context, transaction, table );

        if ( catalogTable.getColumns().size() < 2 ) {
            throw new RuntimeException( "Cannot drop sole column of table " + catalogTable.getTable().name );
        }

        if ( column.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + column.toString() );
        }

        CatalogColumn catalogColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, column );
        try {
            // Check if column is part of an key
            for ( CatalogKey key : catalogTable.getKeys() ) {
                if ( key.columnIds.contains( catalogColumn.id ) ) {
                    CatalogCombinedKey combinedKey = transaction.getCatalog().getCombinedKey( key.id );
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

            List<CatalogColumn> columns = transaction.getCatalog().getColumns( catalogTable.getTable().id );
            transaction.getCatalog().deleteColumn( catalogColumn.id );
            if ( catalogColumn.position != columns.size() ) {
                // Update position of the other columns
                for ( int i = catalogColumn.position; i < columns.size(); i++ ) {
                    transaction.getCatalog().setColumnPosition( columns.get( i ).id, i );
                }
            }

            // Delete column from underlying data stores
            for ( CatalogColumnPlacement dp : catalogTable.getColumnPlacementsByColumn().get( catalogColumn.id ) ) {
                StoreManager.getInstance().getStore( dp.storeId ).dropColumn( context, catalogTable, catalogColumn );
                transaction.getCatalog().deleteColumnPlacement( dp.storeId, dp.columnId );
            }
        } catch ( UnknownTypeException | UnknownCollationException | GenericCatalogException | UnknownKeyException | UnknownTableException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }

}

