/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.transaction.TableAccessMap.TableIdentifier;

// Based on code taken from https://github.com/dstibrany/LockManager
public class LockManager {

    public static final LockManager INSTANCE = new LockManager();
    public static final TableIdentifier GLOBAL_LOCK = new TableIdentifier( -1 ); // For locking whole schema

    private final ConcurrentHashMap<TableIdentifier, Lock> lockTable;
    @Getter
    private final WaitForGraph waitForGraph;


    private LockManager() {
        lockTable = new ConcurrentHashMap<>();
        waitForGraph = new WaitForGraph();
    }


    public void lock( @NonNull TableIdentifier tableIdentifier, @NonNull TransactionImpl transaction, @NonNull Lock.LockMode requestedMode ) throws DeadlockException {
        lockTable.putIfAbsent( tableIdentifier, new Lock( waitForGraph ) );

        Lock lock = lockTable.get( tableIdentifier );

        try {
            if ( hasLock( transaction, tableIdentifier ) && (requestedMode == lock.getMode()) ) {
                return;
            } else if ( requestedMode == Lock.LockMode.SHARED && hasLock( transaction, tableIdentifier ) && lock.getMode() == Lock.LockMode.EXCLUSIVE ) {
                return;
            } else if ( requestedMode == Lock.LockMode.EXCLUSIVE && hasLock( transaction, tableIdentifier ) && lock.getMode() == Lock.LockMode.SHARED ) {
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


    public void unlock( @NonNull TableIdentifier tableIdentifier, @NonNull TransactionImpl transaction ) {
        Lock lock = lockTable.get( tableIdentifier );
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


    public boolean hasLock( @NonNull TransactionImpl transaction, @NonNull TableIdentifier tableIdentifier ) {
        Set<Lock> lockList = transaction.getLocks();
        if ( lockList == null ) {
            return false;
        }
        for ( Lock txnLock : lockList ) {
            if ( txnLock == lockTable.get( tableIdentifier ) ) {
                return true;
            }
        }
        return false;
    }


    Lock.LockMode getLockMode( @NonNull TableIdentifier tableIdentifier ) {
        return lockTable.get( tableIdentifier ).getMode();
    }


}
