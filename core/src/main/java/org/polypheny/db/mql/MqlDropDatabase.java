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

package org.polypheny.db.mql;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.mql.parser.MqlParserPos;
import org.polypheny.db.transaction.Statement;


public class MqlDropDatabase extends MqlNode implements MqlExecutableStatement {

    public MqlDropDatabase( MqlParserPos pos ) {
        super( pos );
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        Catalog catalog = Catalog.getInstance();
        CatalogSchema schema = catalog.getUser( context.getCurrentUserId() ).getDefaultSchema();

        try {
            DdlManager.getInstance().dropSchema( schema.databaseId, database, true, statement );
        } catch ( SchemaNotExistException | DdlOnSourceException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public Type getKind() {
        return Type.DROP_DATABASE;
    }

}
