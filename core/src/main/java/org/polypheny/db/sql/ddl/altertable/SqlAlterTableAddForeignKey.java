/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyOptionException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


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
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        CatalogTable refTable = getCatalogTable( context, referencesTable );

        if ( catalogTable.isView() || refTable.isView() ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE with Views" );
        }

        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( catalogTable.tableType != TableType.TABLE ) {
            throw SqlUtil.newContextException( table.getParserPosition(), RESOURCE.ddlOnSourceTable() );
        }
        if ( refTable.tableType != TableType.TABLE ) {
            throw SqlUtil.newContextException( referencesTable.getParserPosition(), RESOURCE.ddlOnSourceTable() );
        }
        try {
            DdlManager.getInstance().addForeignKey(
                    catalogTable,
                    refTable,
                    columnList.getList().stream().map( SqlNode::toString ).collect( Collectors.toList() ),
                    referencesList.getList().stream().map( SqlNode::toString ).collect( Collectors.toList() ),
                    constraintName.getSimple(),
                    onUpdate,
                    onDelete );
        } catch ( UnknownColumnException e ) {
            throw SqlUtil.newContextException( columnList.getParserPosition(), RESOURCE.columnNotFound( e.getColumnName() ) );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }

}

