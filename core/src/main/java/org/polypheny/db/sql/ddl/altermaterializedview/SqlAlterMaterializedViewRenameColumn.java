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

package org.polypheny.db.sql.ddl.altermaterializedview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterMaterializedView;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;

public class SqlAlterMaterializedViewRenameColumn extends SqlAlterMaterializedView {


    private final SqlIdentifier materializedView;
    private final SqlIdentifier columnOldName;
    private final SqlIdentifier columnNewName;


    public SqlAlterMaterializedViewRenameColumn( SqlParserPos pos, SqlIdentifier materializedView, SqlIdentifier columnOldName, SqlIdentifier columnNewName ) {
        super( pos );
        this.materializedView = Objects.requireNonNull( materializedView );
        this.columnOldName = Objects.requireNonNull( columnOldName );
        this.columnNewName = Objects.requireNonNull( columnNewName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( materializedView, columnNewName, columnOldName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "MATERIALIZED VIEW" );
        materializedView.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "COLUMN" );
        columnOldName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "TO" );
        columnNewName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, materializedView );

        if ( catalogTable.tableType != TableType.MATERIALIZEDVIEW ) {
            throw new RuntimeException( "Not Possible to use ALTER MATERIALIZED VIEW because " + catalogTable.name + " is not a Materialized View." );
        }

        try {
            DdlManager.getInstance().renameColumn( catalogTable, columnOldName.getSimple(), columnNewName.getSimple(), statement );
        } catch ( ColumnAlreadyExistsException e ) {
            throw SqlUtil.newContextException( columnNewName.getParserPosition(), RESOURCE.columnExists( columnNewName.getSimple() ) );
        } catch ( ColumnNotExistsException e ) {
            throw SqlUtil.newContextException( columnOldName.getParserPosition(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        }
    }

}
