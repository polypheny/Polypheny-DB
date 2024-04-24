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

import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


public class MqlRenameCollection extends MqlCollectionStatement implements ExecutableStatement {

    private final String newName;
    private final boolean dropTarget;


    public MqlRenameCollection( ParserPos pos, String collection, String namespace, String newName, Boolean dropTarget ) {
        super( collection, namespace, pos );
        this.newName = newName;
        this.dropTarget = dropTarget != null && dropTarget;
    }


    @Override
    public Type getMqlKind() {
        return Type.RENAME_COLLECTION;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        long namespaceId = parsedQueryContext.getQueryNode().orElseThrow().getNamespaceId();

        LogicalCollection collection = context.getSnapshot().doc().getCollection( namespaceId, getCollection() ).orElseThrow();

        if ( dropTarget ) {
            DdlManager.getInstance().dropCollection( collection, statement );
        }

        DdlManager.getInstance().renameCollection( collection, newName, statement );

    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
