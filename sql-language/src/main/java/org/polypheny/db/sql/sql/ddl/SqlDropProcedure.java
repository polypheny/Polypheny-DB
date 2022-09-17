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

package org.polypheny.db.sql.sql.ddl;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownProcedureException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlOperator;
import org.polypheny.db.sql.sql.SqlSpecialOperator;
import org.polypheny.db.transaction.Statement;


/**
 * Parse tree for {@code DROP PROCEDURE} statement.
 */
public class SqlDropProcedure extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP PROCEDURE", Kind.DROP_PROCEDURE );

    /**
     * Creates a SqlDropFunction.
     */
    public SqlDropProcedure(ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
        
    }

    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        long schemaId;
        String procedureName;
        long databaseId = context.getDatabaseId();
        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.ProcedureName
                schemaId = catalog.getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                procedureName = name.names.get( 2 );
                databaseId = catalog.getDatabase(name.names.get(0)).id;
            } else if ( name.names.size() == 2 ) { // SchemaName.ProcedureName
                schemaId = catalog.getSchema( databaseId, name.names.get( 0 ) ).id;
                procedureName = name.names.get( 1 );
            } else { // ProcedureName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                procedureName = name.names.get(0);
            }
        DdlManager.getInstance().dropProcedure(databaseId, schemaId, procedureName, ifExists);
    } catch (UnknownDatabaseException | UnknownSchemaException | UnknownProcedureException e) {
            throw new RuntimeException(e);
        }
    }

}

