/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.clause.CypherWaitClause;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


@Getter
public class CypherCreateNamespace extends CypherAdminCommand implements ExecutableStatement {

    private final boolean replace;
    private final String namespaceName;
    private final boolean ifNotExists;
    private final CypherWaitClause wait;
    private final CypherSimpleEither<?, ?> options;
    private final String store;


    public CypherCreateNamespace(
            ParserPos pos,
            boolean replace,
            CypherSimpleEither<String, CypherParameter> namespaceName,
            boolean ifNotExists,
            CypherWaitClause wait,
            CypherSimpleEither<?, ?> options,
            CypherSimpleEither<String, CypherParameter> store ) {
        super( pos );
        this.replace = replace;
        this.namespaceName = getNameOrNull( namespaceName );
        this.ifNotExists = ifNotExists;
        this.wait = wait;
        this.options = options;
        this.store = getNameOrNull( store );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        AdapterManager manager = AdapterManager.getInstance();
        if ( wait != null && wait.isWait() ) {
            try {
                Thread.sleep( TimeUnit.MILLISECONDS.convert( wait.getNanos(), TimeUnit.NANOSECONDS ) );
            } catch ( InterruptedException e ) {
                throw new GenericRuntimeException( "While waiting to create the database the operation was interrupted." );
            }
        }

        List<DataStore<?>> dataStore = null;
        if ( store != null ) {
            if ( manager.getStore( store ).isEmpty() ) {
                throw new GenericRuntimeException( "Error while retrieving placement of graph database." );
            }
            dataStore = List.of( manager.getStore( store ).get() );
        }

        if ( Catalog.snapshot().getNamespace( namespaceName ).isPresent() && !ifNotExists ) {
            throw new GenericRuntimeException( "Namespace does already exist" );
        }

        DdlManager.getInstance().createGraph(
                namespaceName,
                true,
                dataStore,
                ifNotExists,
                replace,
                true,
                statement );
    }


    @Override
    public boolean isDdl() {
        return true;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.CREATE_DATABASE;
    }


    @Override
    public @Nullable String getEntity() {
        return namespaceName;
    }

}
