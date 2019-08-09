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


import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.ForeignKeyOption;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownForeignKeyOptionException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code ALTER TABLE name ADD CONSTRAINT FOREIGN KEY} statement.
 */
public class SqlAlterTableAddForeignKeyConstraint extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier constraintName;
    private final SqlNodeList columnList;
    private final SqlIdentifier referencesTable;
    private final SqlNodeList referencesList;
    private final ForeignKeyOption onUpdate;
    private final ForeignKeyOption onDelete;


    public SqlAlterTableAddForeignKeyConstraint( SqlParserPos pos, SqlIdentifier table, SqlIdentifier constraintName, SqlNodeList columnList, SqlIdentifier referencesTable, SqlNodeList referencesList, String onUpdate, String onDelete ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.constraintName = Objects.requireNonNull( constraintName );
        this.columnList = Objects.requireNonNull( columnList );
        this.referencesTable = Objects.requireNonNull( referencesTable );
        this.referencesList = Objects.requireNonNull( referencesList );
        try {
            this.onUpdate = onUpdate != null ? ForeignKeyOption.parse( onUpdate ) : ForeignKeyOption.RESTRICT;
            this.onDelete = onDelete != null ? ForeignKeyOption.parse( onDelete ) : ForeignKeyOption.RESTRICT;
        } catch ( UnknownForeignKeyOptionException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, constraintName, columnList, referencesList );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        constraintName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "FOREIGN" );
        writer.keyword( "KEY" );
        columnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "REFERENCES" );
        referencesList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "UPDATE" );
        writer.keyword( onUpdate.name() );
        writer.keyword( "ON" );
        writer.keyword( "DELETE" );
        writer.keyword( onDelete.name() );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogTable catalogTable = getCatalogTable( context, transaction, table );
        CatalogTable refTable = getCatalogTable( context, transaction, referencesTable );
        try {
            List<Long> columnIds = new LinkedList<>();
            for ( SqlNode node : columnList.getList() ) {
                String columnName = node.toString();
                CatalogColumn catalogColumn = transaction.getCatalog().getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }
            List<Long> referencesIds = new LinkedList<>();
            for ( SqlNode node : referencesList.getList() ) {
                String columnName = node.toString();
                CatalogColumn catalogColumn = transaction.getCatalog().getColumn( refTable.id, columnName );
                referencesIds.add( catalogColumn.id );
            }
            transaction.getCatalog().addForeignKey( catalogTable.id, columnIds, refTable.id, referencesIds, constraintName.getSimple(), onUpdate, onDelete );
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }

}

