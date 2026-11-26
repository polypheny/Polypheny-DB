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


import java.util.function.Function;
import javax.annotation.Nullable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;


public interface TransactionManager {

    Transaction startTransaction( long userId, long defaultNamespaceId, @Nullable QueryAnalyzer analyzer, String origin );

    Transaction startTransaction( long userId, long defaultNamespaceId, @Nullable QueryAnalyzer analyzer, String origin, MultimediaFlavor flavor );

    Transaction startTransaction( long userId, @Nullable QueryAnalyzer analyzer, String origin );

    /**
     * Performs a short catalog-only update in its own transaction.
     * Use the caller's transaction for updates that accompany data or schema changes.
     */
    default <T> T runCatalogWrite( String origin, Function<Transaction, T> action ) {
        Transaction transaction = startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, null, origin );
        try {
            transaction.acquireCatalogWriteLock();
            T result = action.apply( transaction );
            transaction.commit();
            return result;
        } catch ( RuntimeException | Error failure ) {
            if ( transaction.isActive() ) {
                try {
                    transaction.rollback( "Aborting catalog update: " + origin );
                } catch ( RuntimeException | Error rollbackFailure ) {
                    if ( rollbackFailure != failure ) {
                        failure.addSuppressed( rollbackFailure );
                    }
                }
            }
            throw failure;
        }
    }

    void removeTransaction( PolyXid xid );

    boolean isActive( PolyXid xid );

    long getNumberOfActiveTransactions();

    long getNumberOfTotalTransactions();

}
