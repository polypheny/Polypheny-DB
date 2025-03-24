/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.locking;

import java.util.HashMap;
import java.util.List;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.mvcc.MvccUtils;

public class GarbageHandler {

    private static final String TRANSACTION_ORIGIN = "MVCC Garbage Collector";

    private final TransactionManager transactionManager;

    private long lastCleanupSequenceNumber;
    private volatile boolean isRunning;

    private final HashMap<DataModel, GarbageCollector> garbageCollectors;


    public GarbageHandler( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.lastCleanupSequenceNumber = 0;
        this.isRunning = false;
        this.garbageCollectors = new HashMap<>();
        garbageCollectors.put( DataModel.RELATIONAL, new RelGarbageCollector() );
        garbageCollectors.put( DataModel.DOCUMENT, new DocGarbageCollector() );
        garbageCollectors.put( DataModel.GRAPH, new LpgGarbageCollector() );
    }


    public synchronized void runIfRequired( long totalTransactions ) {
        if ( isRunning ) {
            return;
        }

        if ( totalTransactions % RuntimeConfig.GARBAGE_COLLECTION_INTERVAL.getLong() != 0 ) {
            return;
        }

        long lowestActiveTransaction = SequenceNumberGenerator.getInstance().getLowestActive();
        if ( lowestActiveTransaction == lastCleanupSequenceNumber ) {
            return;
        }

        lastCleanupSequenceNumber = lowestActiveTransaction;

        // get all namespaces
        Snapshot snapshot = Catalog.snapshot();
        List<LogicalEntity> entities = snapshot.getNamespaces( null ).stream()
                .filter( MvccUtils::isNamespaceUsingMvcc )
                .flatMap( n -> snapshot.getLogicalEntities( n.getId() ).stream() )
                .filter( Entity::isModifiable )
                .toList();

        if ( entities.isEmpty() ) {
            return;
        }

        // for each entity run cleanup
        isRunning = true;
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, TRANSACTION_ORIGIN );
        entities.forEach( e -> garbageCollectors.get( e.getDataModel() ).collect( e, lowestActiveTransaction, transaction ) );
        transaction.commit();
        isRunning = false;
    }

}
