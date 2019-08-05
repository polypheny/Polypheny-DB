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


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Encoding;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDataPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code ALTER TABLE name DROP COLUMN name} statement.
 */
public class SqlAlterTableAddColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier column;
    private final SqlDataTypeSpec type;
    private final boolean nullable;
    private final SqlIdentifier beforeColumnName; // Can be null
    private final SqlIdentifier afterColumnName; // Can be null


    public SqlAlterTableAddColumn( SqlParserPos pos, SqlIdentifier table, SqlIdentifier column, SqlDataTypeSpec type, boolean nullable, SqlIdentifier beforeColumnName, SqlIdentifier afterColumnName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.column = Objects.requireNonNull( column );
        this.type = Objects.requireNonNull( type );
        this.nullable = nullable;
        this.beforeColumnName = beforeColumnName;
        this.afterColumnName = afterColumnName;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, column, type );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "COLUMN" );
        column.unparse( writer, leftPrec, rightPrec );
        type.unparse( writer, leftPrec, rightPrec );
        if ( nullable ) {
            writer.keyword( "NULL" );
        } else {
            writer.keyword( "NOT NULL" );
        }
        if ( beforeColumnName != null ) {
            writer.keyword( "BEFORE" );
            beforeColumnName.unparse( writer, leftPrec, rightPrec );
        } else if ( afterColumnName != null ) {
            writer.keyword( "AFTER" );
            afterColumnName.unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogCombinedTable catalogTable;
        try {
            catalogTable = transaction.getCatalog().getCombinedTable( getCatalogTable( context, transaction, table ).id );
        } catch ( GenericCatalogException | UnknownTableException e ) {
            throw new RuntimeException( e );
        }

        if ( column.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + column.toString() );
        }
        CatalogColumn beforeColumn = null;
        if ( beforeColumnName != null ) {
            if ( beforeColumnName.names.size() != 1 ) {
                throw new RuntimeException( "No FQDN allowed here: " + beforeColumnName.toString() );
            }
            try {
                beforeColumn = transaction.getCatalog().getColumn( catalogTable.getTable().id, beforeColumnName.getSimple() );
            } catch ( GenericCatalogException e ) {
                throw new RuntimeException( e );
            } catch ( UnknownColumnException e ) {
                throw SqlUtil.newContextException( beforeColumnName.getParserPosition(), RESOURCE.columnNotFound( beforeColumnName.getSimple() ) );
            }
        }
        CatalogColumn afterColumn = null;
        if ( afterColumnName != null ) {
            if ( afterColumnName.names.size() != 1 ) {
                throw new RuntimeException( "No FQDN allowed here: " + afterColumnName.toString() );
            }
            try {
                afterColumn = transaction.getCatalog().getColumn( catalogTable.getTable().id, afterColumnName.getSimple() );
            } catch ( GenericCatalogException e ) {
                throw new RuntimeException( e );
            } catch ( UnknownColumnException e ) {
                throw SqlUtil.newContextException( afterColumnName.getParserPosition(), RESOURCE.columnNotFound( afterColumnName.getSimple() ) );
            }
        }

        // TODO: Check if the column either allows null values or has a default value defined.

        CatalogColumn addedColumn;
        try {
            if ( transaction.getCatalog().checkIfExistsColumn( catalogTable.getTable().id, column.getSimple() ) ) {
                throw SqlUtil.newContextException( column.getParserPosition(), RESOURCE.columnExists( column.getSimple() ) );
            }
            List<CatalogColumn> columns = transaction.getCatalog().getColumns( catalogTable.getTable().id );
            int position = columns.size() + 1;
            if ( beforeColumn != null || afterColumn != null ) {
                if ( beforeColumn != null ) {
                    position = beforeColumn.position;
                } else {
                    position = afterColumn.position + 1;
                }
                // Update position of the other columns
                for ( int i = columns.size(); i >= position; i-- ) {
                    transaction.getCatalog().changeColumnPosition( columns.get( i - 1 ).id, i + 1 );
                }
            }
            long columnId = transaction.getCatalog().addColumn(
                    column.getSimple(),
                    catalogTable.getTable().id,
                    position,
                    PolySqlType.getPolySqlTypeFromSting( type.getTypeName().getSimple() ),
                    type.getScale() == -1 ? null : type.getScale(),
                    type.getPrecision() == -1 ? null : type.getPrecision(),
                    nullable,
                    Encoding.UTF8,
                    Collation.CASE_INSENSITIVE,
                    false
            );
            addedColumn = transaction.getCatalog().getColumn( columnId );
        } catch ( GenericCatalogException | UnknownTypeException | UnknownEncodingException | UnknownCollationException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }

        // Add column on underlying data stores
        for ( CatalogDataPlacement dp : catalogTable.getPlacements() ) {
            StoreManager.getInstance().getStore( dp.storeId ).addColumn( catalogTable, addedColumn );
        }
    }

}

