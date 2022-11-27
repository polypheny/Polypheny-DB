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

package org.polypheny.db.sql.language.ddl;

import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.view.MaterializedViewManager;

public class SqlDropMaterializedView extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP MATERIALIZED VIEW", Kind.DROP_MATERIALIZED_VIEW );


    /**
     * Creates a SqlDropMaterializedView.
     */
    SqlDropMaterializedView( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
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

        if ( catalogTable.entityType != EntityType.MATERIALIZED_VIEW ) {
            throw new RuntimeException( "Not Possible to use DROP MATERIALIZED VIEW because " + catalogTable.name + " is not a Materialized View." );
        }

        MaterializedViewManager materializedManager = MaterializedViewManager.getInstance();
        materializedManager.isDroppingMaterialized = true;
        materializedManager.deleteMaterializedViewFromInfo( catalogTable.id );

        try {
            DdlManager.getInstance().dropMaterializedView( catalogTable, statement );
        } catch ( DdlOnSourceException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.ddlOnSourceTable() );
        }
        materializedManager.isDroppingMaterialized = false;
    }

}
