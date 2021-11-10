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

package org.polypheny.db.languages.mql;

import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.transaction.Statement;


public class MqlUseDatabase extends MqlNode implements MqlExecutableStatement {

    @Getter
    private String database;


    public MqlUseDatabase( ParserPos pos, String database ) {
        super( pos );
        this.database = database;
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        Catalog catalog = Catalog.getInstance();

        int userId = catalog.getUser( Catalog.defaultUser ).id;

        try {
            DdlManager.getInstance().createSchema( this.database, Catalog.defaultDatabaseId, SchemaType.DOCUMENT, Catalog.defaultUser, true, false );
            /*long schemaId = catalog.getSchema( Catalog.defaultDatabaseId, this.database ).id;
            catalog.setUserSchema( userId, schemaId );*/
        } catch ( SchemaAlreadyExistsException e ) {
            throw new RuntimeException( "The schema creation failed" );
        }
    }


    @Override
    public Type getKind() {
        return Type.USE_DATABASE;
    }

}
