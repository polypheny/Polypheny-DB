/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
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


/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTable extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP TABLE", Kind.DROP_TABLE );

    private boolean isAlias = false;
    SqlIdentifier realName;


    /**
     * Creates a SqlDropTable.
     */
    SqlDropTable( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }

    SqlDropTable( ParserPos pos, boolean ifExists, SqlIdentifier name, boolean isAlias ) {
        super( OPERATOR, pos, ifExists, name );
        this.isAlias = isAlias;
        if ( isAlias ) {
            realName = (SqlIdentifier) replaceTableNameIfIsAlias( name );
        } else {
            realName = name;
        }
    }



    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        final CatalogTable table;
        Catalog catalog = Catalog.getInstance();

        try {
            if ( isAlias ) {
                table = getCatalogTable( context, realName );
                Object[] realTableNameArray = catalog.getTableNameFromAlias(name.names.get(0));
                // now we delete all the alias for this table.
                catalog.removeAliases( new Object[]{ table.databaseId, (long)realTableNameArray[1], table.name } );
            } else {
                table = getCatalogTable( context, name );
            }
        } catch ( PolyphenyDbContextException e ) {
            if ( ifExists ) {
                // It is ok that there is no database / schema / table with this name because "IF EXISTS" was specified
                return;
            } else {
                throw e;
            }
        }

        try {
            DdlManager.getInstance().dropTable( table, statement );
        } catch ( DdlOnSourceException e ) {
            throw CoreUtil.newContextException( name.getPos(), RESOURCE.ddlOnSourceTable() );
        }

    }

}

