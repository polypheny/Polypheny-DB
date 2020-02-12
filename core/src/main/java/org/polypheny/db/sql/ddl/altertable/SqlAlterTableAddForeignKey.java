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
public class SqlAlterTableAddForeignKey extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier constraintName;
    private final SqlNodeList columnList;
    private final SqlIdentifier referencesTable;
    private final SqlNodeList referencesList;
    private final ForeignKeyOption onUpdate;
    private final ForeignKeyOption onDelete;


    public SqlAlterTableAddForeignKey( SqlParserPos pos, SqlIdentifier table, SqlIdentifier constraintName, SqlNodeList columnList, SqlIdentifier referencesTable, SqlNodeList referencesList, String onUpdate, String onDelete ) {
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
        writer.keyword( "CONSTRAINT" );
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

