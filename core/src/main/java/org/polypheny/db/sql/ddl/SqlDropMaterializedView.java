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

package org.polypheny.db.sql.ddl;

import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.view.MaterializedViewManager;

public class SqlDropMaterializedView extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP MATERIALIZED VIEW", SqlKind.DROP_MATERIALIZED_VIEW );


    /**
     * Creates a SqlDropMaterializedView.
     */
    SqlDropMaterializedView( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        final CatalogTable catalogTable;

        try {
            catalogTable = getCatalogTable( context, name );
        } catch ( PolyphenyDbContextException e ) {
            if ( ifExists ) {
                // It is ok that there is no database / schema / table with this name because "IF EXISTS" was specified
                return;
            } else {
                throw e;
            }
        }

        if ( catalogTable.tableType != TableType.MATERIALIZED_VIEW ) {
            throw new RuntimeException( "Not Possible to use DROP MATERIALIZED VIEW because " + catalogTable.name + " is not a Materialized View." );
        }

        MaterializedViewManager materializedManager = MaterializedViewManager.getInstance();
        materializedManager.isDroppingMaterialized = true;
        materializedManager.deleteMaterializedViewFromInfo( catalogTable.id );

        try {
            DdlManager.getInstance().dropMaterializedView( catalogTable, statement );
        } catch ( DdlOnSourceException e ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.ddlOnSourceTable() );
        }
        materializedManager.isDroppingMaterialized = false;
    }

}
