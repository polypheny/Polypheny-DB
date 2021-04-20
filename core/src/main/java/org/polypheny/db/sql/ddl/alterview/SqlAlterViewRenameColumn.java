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

package org.polypheny.db.sql.ddl.alterview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterView;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;

public class SqlAlterViewRenameColumn extends SqlAlterView {

    private final SqlIdentifier view;
    private final SqlIdentifier columnOldName;
    private final SqlIdentifier columnNewName;

    public SqlAlterViewRenameColumn( SqlParserPos pos, SqlIdentifier view, SqlIdentifier columnOldName, SqlIdentifier columnNewName){
        super(pos);
        this.view = Objects.requireNonNull( view );
        this.columnOldName = Objects.requireNonNull( columnOldName );
        this.columnNewName = Objects.requireNonNull( columnNewName );
    }

    @Override
    public List<SqlNode> getOperandList(){
        return ImmutableNullableList.of(view, columnNewName, columnOldName);
    }

    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ){
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
    public void execute( Context context, Statement statement ){
        CatalogView catalogView = (CatalogView)getCatalogTable( context, view );

        try {
            DdlManager.getInstance().renameColumn( catalogView, columnOldName.getSimple(), columnNewName.getSimple(), statement );
        } catch ( ColumnAlreadyExistsException e ) {
            throw SqlUtil.newContextException( columnNewName.getParserPosition(), RESOURCE.columnExists( columnNewName.getSimple() ) );
        } catch ( ColumnNotExistsException e ) {
            throw SqlUtil.newContextException( columnOldName.getParserPosition(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        }
    }

}
