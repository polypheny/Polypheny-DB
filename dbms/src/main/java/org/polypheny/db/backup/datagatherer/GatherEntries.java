/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.backup.datagatherer;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class GatherEntries {
    private final TransactionManager transactionManager;

    public GatherEntries( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }

    public void start() {

        log.debug( "gather entries" );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;
        String query = "";

        try {

            // get a transaction and a statement
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
            statement = transaction.createStatement();
            ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement ).get( 0 );
            // in case of results
            ResultIterator iter = executedQuery.getIterator();
            while ( iter.hasMoreRows() ) {
                // liste mit tuples
                iter.getNextBatch();
            }

        } catch ( Exception e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    // Gather entries with select statements

}
