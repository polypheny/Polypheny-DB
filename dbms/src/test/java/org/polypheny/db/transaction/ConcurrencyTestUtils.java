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

import java.util.List;
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
        System.out.println( query + ":::" + transaction );
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

}
