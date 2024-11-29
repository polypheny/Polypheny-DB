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

package org.polypheny.db.transaction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;

public class ConcurrencyTestUtils {

    public static final String DEFAULT_NAMESPACE = "public";


    public static List<ExecutedContext> executeStatement( String query, String languageName, Transaction transaction, TestHelper testHelper ) {
        return executeStatement( query, languageName, DEFAULT_NAMESPACE, transaction, testHelper );
    }


    public static List<ExecutedContext> executeStatement( String query, String languageName, String namespaceName, Transaction transaction, TestHelper testHelper ) {
        QueryLanguage language = QueryLanguage.from( languageName );
        long namespaceId = Catalog.getInstance().getSnapshot().getNamespace( namespaceName ).orElseThrow().getId();
        QueryContext context = QueryContext.builder()
                .query( query )
                .language( language )
                .namespaceId( namespaceId )
                .transactionManager( testHelper.getTransactionManager() )
                .origin( "IsolationTests" )
                .build();
        return LanguageManager.getINSTANCE().anyQuery( context.addTransaction( transaction ) );
    }


    public static void executePermutations( List<String> executions, Map<String, Runnable> operations, Set<Session> sessions, Runnable setup, Runnable cleanup ) {
        executions.forEach( sequence -> {
            System.out.println( "Executing:" + sequence );
            setup.run();
            Arrays.stream( sequence.split( " " ) )
                    .forEach(
                            operations::get );
            sessions.forEach( session -> {
                try {
                    if (!session.awaitCompletion()) {
                        throw new RuntimeException( "Session did not complete properly." );
                    }
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( "Execution of operation sequence failed.", e );
                }
            } );
            cleanup.run();
        } );

    }

}
