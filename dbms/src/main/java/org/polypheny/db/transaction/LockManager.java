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


import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier.NamespaceLevel;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.util.DeadlockException;


// Based on code taken from https://github.com/dstibrany/LockManager
@Slf4j
public class LockManager {

    public static final LockManager INSTANCE = new LockManager();
    public static final EntityIdentifier GLOBAL_LOCK = new EntityIdentifier( -1L, -1L, NamespaceLevel.ENTITY_LEVEL ); // For locking whole schema

    private final ConcurrentHashMap<EntityIdentifier, Lock> lockTable;
    @Getter
    private final WaitForGraph waitForGraph;


    private LockManager() {
        lockTable = new ConcurrentHashMap<>();
        waitForGraph = new WaitForGraph();
    }


    public void lock( @NonNull Collection<Entry<EntityIdentifier, LockMode>> idAccessMap, @NonNull TransactionImpl transaction ) throws DeadlockException {
        // Decide on which locking  approach to focus
        if ( transaction.acceptsOutdated() ) {
            handleSecondaryLocks( idAccessMap, transaction );
        } else {
            handlePrimaryLocks( idAccessMap, transaction );
        }
    }


    /**
     * Used in traditional transactional workload to lck all entities that will eagerly receive any update
     */
    private void handlePrimaryLocks( @NonNull Collection<Entry<EntityIdentifier, LockMode>> idAccessMap, @NonNull TransactionImpl transaction ) throws DeadlockException {
        Iterator<Entry<EntityIdentifier, LockMode>> iter = idAccessMap.stream().sorted( ( a, b ) -> Math.toIntExact( a.getKey().entityId - b.getKey().entityId ) ).iterator();
        Entry<EntityIdentifier, LockMode> pair;
        while ( iter.hasNext() ) {
            pair = iter.next();
            lockTable.putIfAbsent( pair.getKey(), new Lock( waitForGraph ) );

            Lock lock = lockTable.get( pair.getKey() );

            try {
                if ( hasLock( transaction, pair.getKey() ) && pair.getValue() == lock.getMode() ) {
                    continue;
                } else if ( pair.getValue() == Lock.LockMode.SHARED && hasLock( transaction, pair.getKey() ) && lock.getMode() == Lock.LockMode.EXCLUSIVE ) {
                    continue;
                } else if ( pair.getValue() == Lock.LockMode.EXCLUSIVE && hasLock( transaction, pair.getKey() ) && lock.getMode() == Lock.LockMode.SHARED ) {
                    lock.upgrade( transaction );
                } else {
                    lock.acquire( transaction, pair.getValue() );
                }
            } catch ( InterruptedException e ) {
                removeTransaction( transaction );
                throw new DeadlockException( e );
            }

            transaction.addLock( lock );
        }
    }


    /**
     * Used in freshness related workload to lock all entities that will lazily receive updates (considered secondaries)
     */
    private void handleSecondaryLocks( @NonNull Collection<Entry<EntityIdentifier, LockMode>> idAccessMap, @NonNull TransactionImpl transaction ) throws DeadlockException {
        // Try locking secondaries first.
        // If this cannot be fulfilled by data distribution fallback and try to acquire a regular primary lock
        // TODO @HENNLO Check if this decision should even be made here or somewhere else

        // This is mainly relevant for Queries on secondaries/outdated nodes.
        // In theory, we already know for each query which partitions are going to be accessed.
        // The FreshnessManager could therefore already be invoked prior to Routing to decide if the Freshness can be
        // guaranteed or if we need to fall back to primary locking mechanisms.
    }


    public void unlock( @NonNull Collection<EntityIdentifier> ids, @NonNull TransactionImpl transaction ) {
        Iterator<EntityIdentifier> iter = ids.iterator();
        EntityIdentifier entityIdentifier;
        while ( iter.hasNext() ) {
            entityIdentifier = iter.next();
            Lock lock = lockTable.get( entityIdentifier );
            if ( lock != null ) {
                lock.release( transaction );
            }
            transaction.removeLock( lock );
        }
    }


    public void removeTransaction( @NonNull TransactionImpl transaction ) {
        Set<Lock> txnLockList = transaction.getLocks();
        for ( Lock lock : txnLockList ) {
            lock.release( transaction );
        }
    }


    public boolean hasLock( @NonNull TransactionImpl transaction, @NonNull EntityAccessMap.EntityIdentifier entityIdentifier ) {
        Set<Lock> locks = transaction.getLocks();
        if ( locks == null ) {
            return false;
        }
        for ( Lock txnLock : locks ) {
            if ( txnLock == lockTable.get( entityIdentifier ) ) {
                return true;
            }
        }
        return false;
    }


    Lock.LockMode getLockMode( @NonNull EntityAccessMap.EntityIdentifier entityIdentifier ) {
        return lockTable.get( entityIdentifier ).getMode();
    }


    public Map<EntityIdentifier, Lock> getLocks() {
        return Map.copyOf( lockTable );
    }

}
