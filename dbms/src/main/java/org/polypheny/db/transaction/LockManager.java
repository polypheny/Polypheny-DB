/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier;
import org.polypheny.db.util.DeadlockException;

// Based on code taken from https://github.com/dstibrany/LockManager
public class LockManager {

    public static final LockManager INSTANCE = new LockManager();
    public static final EntityIdentifier GLOBAL_LOCK = new EntityIdentifier( -1L, -1L ); // For locking whole schema

    private final ConcurrentHashMap<EntityIdentifier, Lock> lockTable;
    @Getter
    private final WaitForGraph waitForGraph;


    private LockManager() {
        lockTable = new ConcurrentHashMap<>();
        waitForGraph = new WaitForGraph();
    }


    public void lock( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier, @NonNull TransactionImpl transaction, @NonNull Lock.LockMode requestedMode ) throws DeadlockException {

        //TODO @HENNLO Make sure that if we need to re-acquire a lock for PRIMARY in routing
        // We need a lever to fall back to the 'else' part, because the TX still "acceptsOutdated"
        // Depends if we also accept DIRTY-READS for every freshness specified read

        // Decide on which locking  approach to focus
        if ( transaction.acceptsOutdatedCopies() ) {
            handleSecondaryLocks( entityIdentifier, transaction, requestedMode );
        } else {
            handlePrimaryLocks( entityIdentifier, transaction, requestedMode );
        }

    }


    /**
     * Used in traditional transactional workload to lock all entities that will eagerly receive any update
     */
    private void handlePrimaryLocks( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier, @NonNull TransactionImpl transaction, @NonNull Lock.LockMode requestedMode ) throws DeadlockException {
        lockTable.putIfAbsent( entityIdentifier, new Lock( waitForGraph ) );

        Lock lock = lockTable.get( entityIdentifier );

        try {
            if ( hasLock( transaction, entityIdentifier ) && (requestedMode == lock.getMode()) ) {
                return;
            } else if ( requestedMode == Lock.LockMode.SHARED && hasLock( transaction, entityIdentifier ) && lock.getMode() == Lock.LockMode.EXCLUSIVE ) {
                return;
            } else if ( requestedMode == Lock.LockMode.EXCLUSIVE && hasLock( transaction, entityIdentifier ) && lock.getMode() == Lock.LockMode.SHARED ) {
                lock.upgrade( transaction );
            } else {
                lock.acquire( transaction, requestedMode );
            }
        } catch ( InterruptedException e ) {
            removeTransaction( transaction );
            throw new DeadlockException( e );
        }

        transaction.addLock( lock );
    }


    /**
     * Used in freshness related workload to lock all entities that will lazily receive updates (considered secondaries)
     */
    private void handleSecondaryLocks( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier, @NonNull TransactionImpl transaction, @NonNull Lock.LockMode requestedMode ) {
        // Try locking secondaries first.
        // If this cannot be fulfilled by data distribution fallback and try to acquire a regular primary lock
        // TODO @HENNLO Check if this decision should even be made here or somewhere else

        // This is mainly relevant for Queries on secondaries/outdated nodes.
        // In Theory we already know for each query which partitions are going to be accessed.
        // The FreshnessManager could therefore already be invoked prior to Routing to decide if the Freshness can be
        // guaranteed or if we need to fall back to primary locking mechanisms.

    }


    public void unlock( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier, @NonNull TransactionImpl transaction ) {
        Lock lock = lockTable.get( entityIdentifier );
        if ( lock != null ) {
            lock.release( transaction );
        }
        transaction.removeLock( lock );
    }


    public void removeTransaction( @NonNull TransactionImpl transaction ) {
        Set<Lock> txnLockList = transaction.getLocks();
        for ( Lock lock : txnLockList ) {
            lock.release( transaction );
        }
    }


    public boolean hasLock( @NonNull TransactionImpl transaction, @NonNull EntityAccessMap.EntityIdentifier entityIdentifier ) {
        Set<Lock> lockList = transaction.getLocks();
        if ( lockList == null ) {
            return false;
        }
        for ( Lock txnLock : lockList ) {
            if ( txnLock == lockTable.get( entityIdentifier ) ) {
                return true;
            }
        }
        return false;
    }


    public Lock.LockMode getLockMode( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier ) {
        return lockTable.get( entityIdentifier ).getMode();
    }


}
