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

package org.polypheny.db.processing;

import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

@Value
public class QueryContext {

    String query;

    QueryLanguage language;

    boolean isAnalysed;

    boolean usesCache;

    long userId;

    String origin;

    int batch; // -1 for all

    TransactionManager manager;

    @NonFinal
    Transaction transaction;


    public Transaction openTransaction() {
        return manager.startTransaction( userId, Catalog.defaultNamespaceId, isAnalysed, origin );
    }


    public Transaction openTransaction( long namespaceId ) {
        return manager.startTransaction( userId, namespaceId, isAnalysed, origin );
    }


}
