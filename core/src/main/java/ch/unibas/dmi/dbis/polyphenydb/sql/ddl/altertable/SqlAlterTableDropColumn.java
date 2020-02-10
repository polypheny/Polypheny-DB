/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl.altertable;


import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumnPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownKeyException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import java.util.Objects;


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
        } catch ( UnknownTypeException | UnknownCollationException | GenericCatalogException | UnknownKeyException e ) {
            throw new RuntimeException( e );
        }
    }

}

