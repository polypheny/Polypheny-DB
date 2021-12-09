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

package org.polypheny.db.sql.sql.ddl.altermaterializedview;

import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterMaterializedView;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


public class SqlAlterMaterializedViewRename extends SqlAlterMaterializedView {

    private final SqlIdentifier oldName;
    private final SqlIdentifier newName;


    /**
     * Creates a SqlAlterMaterializedViewRename.
     */
    public SqlAlterMaterializedViewRename( ParserPos pos, SqlIdentifier oldName, SqlIdentifier newName ) {
        super( pos );
        this.oldName = Objects.requireNonNull( oldName );
        this.newName = Objects.requireNonNull( newName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( oldName, newName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( oldName, newName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "MATERIALIZED VIEW" );
        oldName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "TO" );
        newName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, oldName );

        if ( catalogTable.tableType != TableType.MATERIALIZED_VIEW ) {
            throw new RuntimeException( "Not Possible to use ALTER MATERIALIZED VIEW because " + catalogTable.name + " is not a Materialized View." );
        }

        if ( newName.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + newName.toString() );
        }
        try {
            DdlManager.getInstance().renameTable( catalogTable, newName.getSimple(), statement );
        } catch ( TableAlreadyExistsException e ) {
            throw CoreUtil.newContextException( oldName.getPos(), RESOURCE.schemaExists( newName.getSimple() ) );
        }
    }

}
