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
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.clause.CypherWaitClause;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


public class CypherDropNamespace extends CypherAdminCommand implements ExecutableStatement {

    private final String databaseName;
    private final boolean ifExists;
    private final boolean dumpData;
    private final CypherWaitClause wait;


    public CypherDropNamespace(
            ParserPos pos,
            CypherSimpleEither<String, CypherParameter> databaseName,
            boolean ifExists,
            boolean dumpData,
            CypherWaitClause wait ) {
        super( pos );
        this.databaseName = databaseName.getLeft() != null ? databaseName.getLeft() : databaseName.getRight().getName();
        this.ifExists = ifExists;
        this.dumpData = dumpData;
        this.wait = wait;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        if ( wait != null && wait.isWait() ) {
            try {
                Thread.sleep( TimeUnit.MILLISECONDS.convert( wait.getNanos(), TimeUnit.NANOSECONDS ) );
            } catch ( InterruptedException e ) {
                throw new UnsupportedOperationException( "While waiting to create the database the operation was interrupted." );
            }
        }

        List<LogicalNamespace> databases = statement.getTransaction().getSnapshot().getNamespaces( new Pattern( databaseName ) );

        if ( databases.size() != 1 ) {
            if ( !ifExists ) {
                throw new GenericRuntimeException( "Graph database does not exist and IF EXISTS was not specified." );
            }
            return;
        }

        DdlManager.getInstance().dropGraph( databases.get( 0 ).id, ifExists, statement );
    }


    @Override
    public boolean isDdl() {
        return true;
    }


    @Override
    public @Nullable String getEntity() {
        return databaseName;
    }

}
