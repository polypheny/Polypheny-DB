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

package org.polypheny.db.languages.mql;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


public class MqlDropDatabase extends MqlNode implements ExecutableStatement {

    public MqlDropDatabase( ParserPos pos ) {
        super( pos );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        String database = ((MqlQueryParameters) parameters).getDatabase();

        try {
            DdlManager.getInstance().dropSchema( Catalog.defaultDatabaseId, database, true, statement );
        } catch ( SchemaNotExistException | DdlOnSourceException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public Type getMqlKind() {
        return Type.DROP_DATABASE;
    }

}
