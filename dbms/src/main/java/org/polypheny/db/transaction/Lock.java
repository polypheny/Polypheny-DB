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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction.AccessMode;


// Based on code taken from https://github.com/dstibrany/LockManager
public class Lock {

    private final Set<TransactionImpl> owners = new HashSet<>();
    private final ReentrantLock lock = new ReentrantLock( true );
    private final Condition waiters = lock.newCondition();
    private final WaitForGraph waitForGraph;
    private int xLockCount = 0;
    private int sLockCount = 0;


    Lock( WaitForGraph waitForGraph ) {
        this.waitForGraph = waitForGraph;
    }


    void acquire( TransactionImpl txn, LockMode lockMode ) throws InterruptedException {
        if ( lockMode == LockMode.SHARED ) {
            acquireSLock( txn );
            txn.updateAccessMode( AccessMode.READ_ACCESS );
        } else if ( lockMode == LockMode.EXCLUSIVE ) {
            acquireXLock( txn );
            txn.updateAccessMode( AccessMode.WRITE_ACCESS );
        } else {
            throw new GenericRuntimeException( "Lock mode does not exist" );
        }
    }


    void release( TransactionImpl txn ) {
        lock.lock();
        try {
            if ( sLockCount > 0 ) {
                sLockCount--;
            }
            if ( xLockCount == 1 ) {
                xLockCount = 0;
            }

            owners.remove( txn );
            waitForGraph.remove( txn );

            waiters.signalAll();
        } finally {
            lock.unlock();
        }
    }


    void upgrade( TransactionImpl txn ) throws InterruptedException {
        lock.lock();
        try {
            if ( owners.contains( txn ) && isXLocked() ) {
                return;
            }
            while ( isXLocked() || sLockCount > 1 ) {
                Set<TransactionImpl> ownersWithSelfRemoved = owners.stream().filter( ownerTxn -> !ownerTxn.equals( txn ) ).collect( Collectors.toSet() );
                waitForGraph.add( txn, ownersWithSelfRemoved );
                waitForGraph.detectDeadlock( txn );
                waiters.await();
            }
            sLockCount = 0;
            xLockCount = 1;
        } finally {
            lock.unlock();
        }
    }


    LockMode getMode() {
        LockMode lockMode = null;
        lock.lock();

        try {
            if ( isXLocked() ) {
                lockMode = LockMode.EXCLUSIVE;
            } else if ( isSLocked() ) {
                lockMode = LockMode.SHARED;
            }
        } finally {
            lock.unlock();
        }

        return lockMode;
    }


    Set<TransactionImpl> getOwners() {
        return owners;
    }


    private void acquireSLock( TransactionImpl txn ) throws InterruptedException {
        lock.lock();
        try {
            while ( isXLocked() || lock.hasWaiters( waiters ) ) {
                waitForGraph.add( txn, owners );
                waitForGraph.detectDeadlock( txn );
                waiters.await();
            }
            sLockCount++;
            owners.add( txn );
        } finally {
            lock.unlock();
        }
    }


    private void acquireXLock( TransactionImpl txn ) throws InterruptedException {
        lock.lock();
        try {
            while ( isXLocked() || isSLocked() ) {
                waitForGraph.add( txn, owners );
                waitForGraph.detectDeadlock( txn );
                waiters.await();
            }
            xLockCount = 1;
            owners.add( txn );
        } finally {
            lock.unlock();
        }
    }


    private boolean isXLocked() {
        return xLockCount == 1;
    }


    private boolean isSLocked() {
        return sLockCount > 0;
    }


    public enum LockMode {
        SHARED,
        EXCLUSIVE
    }

}
