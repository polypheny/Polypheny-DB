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

import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.exceptions.NamespaceAlreadyExistsException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


public class MqlUseDatabase extends MqlNode implements ExecutableStatement {

    @Getter
    private final String database;


    public MqlUseDatabase( ParserPos pos, String database ) {
        super( pos );
        this.database = database;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        try {
            DdlManager.getInstance().createNamespace( this.database, Catalog.defaultDatabaseId, NamespaceType.DOCUMENT, Catalog.defaultUserId, true, false );
        } catch ( NamespaceAlreadyExistsException e ) {
            throw new RuntimeException( "The schema creation failed" );
        }
    }


    @Override
    public Type getMqlKind() {
        return Type.USE_DATABASE;
    }

}
