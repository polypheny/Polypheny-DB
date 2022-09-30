/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl.alterview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterView;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;

public class SqlAlterViewRenameColumn extends SqlAlterView {

    private final SqlIdentifier view;
    private final SqlIdentifier columnOldName;
    private final SqlIdentifier columnNewName;


    public SqlAlterViewRenameColumn( ParserPos pos, SqlIdentifier view, SqlIdentifier columnOldName, SqlIdentifier columnNewName ) {
        super( pos );
        this.view = Objects.requireNonNull( view );
        this.columnOldName = Objects.requireNonNull( columnOldName );
        this.columnNewName = Objects.requireNonNull( columnNewName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( view, columnNewName, columnOldName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( view, columnNewName, columnOldName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "VIEW" );
        view.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "COLUMN" );
        columnOldName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "TO" );
        columnNewName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, view );

        if ( catalogTable.entityType != EntityType.VIEW ) {
            throw new RuntimeException( "Not Possible to use ALTER VIEW because " + catalogTable.name + " is not a View." );
        }

        try {
            DdlManager.getInstance().renameColumn( catalogTable, columnOldName.getSimple(), columnNewName.getSimple(), statement );
        } catch ( ColumnAlreadyExistsException e ) {
            throw CoreUtil.newContextException( columnNewName.getPos(), RESOURCE.columnExists( columnNewName.getSimple() ) );
        } catch ( ColumnNotExistsException e ) {
            throw CoreUtil.newContextException( columnOldName.getPos(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        }
    }

}
