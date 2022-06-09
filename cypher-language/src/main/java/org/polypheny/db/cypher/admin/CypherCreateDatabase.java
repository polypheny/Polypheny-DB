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

package org.polypheny.db.cypher.admin;

import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.clause.CypherWaitClause;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;

@Getter
public class CypherCreateDatabase extends CypherAdminCommand implements ExecutableStatement {

    private final boolean replace;
    private final String databaseName;
    private final boolean ifNotExists;
    private final CypherWaitClause wait;
    private final CypherSimpleEither options;


    public CypherCreateDatabase( ParserPos pos, boolean replace, CypherSimpleEither<String, CypherParameter> databaseName, boolean ifNotExists, CypherWaitClause wait, CypherSimpleEither options ) {
        super( pos );
        this.replace = replace;
        if ( databaseName.getLeft() != null ) {
            this.databaseName = databaseName.getLeft();
        } else {
            this.databaseName = databaseName.getRight().getName();
        }
        this.ifNotExists = ifNotExists;
        this.wait = wait;
        this.options = options;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        if ( wait != null && wait.isWait() ) {
            try {
                Thread.sleep( TimeUnit.MILLISECONDS.convert( wait.getNanos(), TimeUnit.NANOSECONDS ) );
            } catch ( InterruptedException e ) {
                throw new UnsupportedOperationException( "While waiting to create the database the operation was interrupted." );
            }
        }

        DdlManager.getInstance().createGraphDatabase( Catalog.defaultDatabaseId, databaseName, true, null, ifNotExists, replace, statement );

    }


    @Override
    public boolean isDDL() {
        return true;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.CREATE_DATABASE;
    }

}
