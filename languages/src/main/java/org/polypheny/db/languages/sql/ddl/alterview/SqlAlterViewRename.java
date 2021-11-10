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

package org.polypheny.db.languages.sql.ddl.alterview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlUtil;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.ddl.SqlAlterView;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER TABLE name RENAME TO} statement
 */
public class SqlAlterViewRename extends SqlAlterView {

    private final SqlIdentifier oldName;
    private final SqlIdentifier newName;


    /**
     * Creates a SqlAlterViewRename.
     */
    public SqlAlterViewRename( ParserPos pos, SqlIdentifier oldName, SqlIdentifier newName ) {
        super( pos );
        this.oldName = Objects.requireNonNull( oldName );
        this.newName = Objects.requireNonNull( newName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( oldName, newName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "VIEW" );
        oldName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "TO" );
        newName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, oldName );

        if ( catalogTable.tableType != TableType.VIEW ) {
            throw new RuntimeException( "Not Possible to use ALTER VIEW because " + catalogTable.name + " is not a View." );
        }

        if ( newName.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + newName.toString() );
        }
        try {
            DdlManager.getInstance().renameTable( catalogTable, newName.getSimple(), statement );
        } catch ( TableAlreadyExistsException e ) {
            throw SqlUtil.newContextException( oldName.getPos(), RESOURCE.schemaExists( newName.getSimple() ) );
        }
    }

}
