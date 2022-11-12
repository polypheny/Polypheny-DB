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

import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
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
    private final String store;


    public CypherCreateDatabase(
            ParserPos pos,
            boolean replace,
            CypherSimpleEither<String, CypherParameter> databaseName,
            boolean ifNotExists,
            CypherWaitClause wait,
            CypherSimpleEither options,
            CypherSimpleEither<String, CypherParameter> store ) {
        super( pos );
        this.replace = replace;
        this.databaseName = getNameOrNull( databaseName );
        this.ifNotExists = ifNotExists;
        this.wait = wait;
        this.options = options;
        this.store = getNameOrNull( store );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        AdapterManager manager = AdapterManager.getInstance();
        if ( wait != null && wait.isWait() ) {
            try {
                Thread.sleep( TimeUnit.MILLISECONDS.convert( wait.getNanos(), TimeUnit.NANOSECONDS ) );
            } catch ( InterruptedException e ) {
                throw new UnsupportedOperationException( "While waiting to create the database the operation was interrupted." );
            }
        }

        List<DataStore> dataStore = null;
        if ( store != null ) {
            if ( manager.getStore( store ) == null ) {
                throw new RuntimeException( "Error while retrieving placement of graph database." );
            }
            dataStore = List.of( manager.getStore( store ) );
        }

        DdlManager.getInstance().createGraph(
                Catalog.defaultDatabaseId,
                databaseName,
                true,
                dataStore,
                ifNotExists,
                replace,
                statement );
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
