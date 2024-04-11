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

package org.polypheny.db.languages.mql;

import java.util.List;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


public class MqlDrop extends MqlCollectionStatement implements ExecutableStatement {

    public MqlDrop( ParserPos pos, String collection, String namespace ) {
        super( collection, namespace, pos );
    }


    @Override
    public Type getMqlKind() {
        return Type.DROP;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        DdlManager ddlManager = DdlManager.getInstance();
        long namespaceId = parsedQueryContext.getNamespaceId();

        Optional<LogicalNamespace> optionalNamespace = context.getSnapshot().getNamespace( namespaceId );
        if ( optionalNamespace.isEmpty() ) {
            // dropping a document database( namespace ), which does not exist, which is a no-op
            return;
        }

        LogicalNamespace namespace = optionalNamespace.get();
        List<LogicalCollection> collections = context.getSnapshot().doc().getCollections( namespace.id, new Pattern( collection ) );
        if ( collections.size() != 1 ) {
            // dropping a collection, which does not exist, which is a no-op
            return;
        }
        ddlManager.dropCollection( collections.get( 0 ), statement );
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
