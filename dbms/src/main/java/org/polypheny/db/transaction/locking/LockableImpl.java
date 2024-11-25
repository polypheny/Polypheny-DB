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

package org.polypheny.db.transaction.locking;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.deadlocks.DeadlockHandler;
import org.polypheny.db.util.DeadlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockableImpl implements Lockable {
    private static final Logger LOGGER = LoggerFactory.getLogger( LockableImpl.class );

    private final ReentrantLock concurrencyLock = new ReentrantLock( true );
    private final Condition concurrencyCondition = concurrencyLock.newCondition();
    private final HashMap<Transaction, Long> owners = new HashMap<>();
    Lockable parent;

    private boolean isExclusive = false;


    public LockableImpl( Lockable Parent ) {
        this.parent = Parent;
    }


    public void acquire( @NotNull Transaction transaction, @NotNull LockType lockType ) throws DeadlockException {
        try {
            if ( !owners.containsKey( transaction ) ) {
                switch ( lockType ) {
                    case SHARED -> acquireShared( transaction );
                    case EXCLUSIVE -> acquireExclusive( transaction );
                }
                return;
            }
            LockType heldLockType = getLockType();
            if ( heldLockType == lockType ) {
                return;
            }
            if ( heldLockType == LockType.SHARED ) {
                upgradeToExclusive( transaction );
            }
        } catch ( InterruptedException e ) {
            transaction.releaseAllLocks();
            throw new DeadlockException( MessageFormat.format( "Transaction {0} encountered a deadlock while acquiring a lock of type {1} on entry {2}.", transaction.getId(), lockType, this ) );
        }
    }


    private void upgradeToExclusive( Transaction transaction ) throws InterruptedException {
        concurrencyLock.lock();
        try {
            if ( isExclusive ) {
                return;
            }
            long count = owners.remove( transaction );
            while ( !owners.isEmpty() ) {
                DeadlockHandler.INSTANCE.addAndResolveDeadlock(this, transaction, owners.keySet() );
                concurrencyCondition.await();
            }
            isExclusive = true;
            owners.put( transaction, count );
        } finally {
            concurrencyLock.unlock();
        }
    }


    public void release( @NotNull Transaction transaction ) {
        concurrencyLock.lock();
        try {
            if ( isExclusive ) {
                owners.clear();
                isExclusive = false;
            }
            // this decrements the entry if > 1 else it is removed
            owners.computeIfPresent( transaction, ( key, value ) -> {
                long newValue = value - 1;
                return newValue <= 0 ? null : newValue;
            } );
            DeadlockHandler.INSTANCE.remove( this, transaction );
            concurrencyCondition.signalAll();
            printInfo( "R", transaction );
        } finally {
            concurrencyLock.unlock();
        }
        if ( !isRoot() ) {
            parent.release( transaction );
        }
    }


    @Override
    public LockType getLockType() {
        return isExclusive ? LockType.EXCLUSIVE : LockType.SHARED;
    }


    @Override
    public boolean isRoot() {
        return parent == null;
    }


    private void acquireShared( Transaction transaction ) throws InterruptedException {
        if ( !isRoot() ) {
            parent.acquire( transaction, LockType.SHARED );
        }
        concurrencyLock.lock();
        try {
            while ( isExclusive || hasWaitingTransactions() ) {
                DeadlockHandler.INSTANCE.addAndResolveDeadlock(this, transaction, owners.keySet() );
                concurrencyCondition.await();
            }
            owners.put( transaction, owners.getOrDefault( transaction, 0L ) + 1 );
            printInfo( "ASh", transaction );
        } finally {
            concurrencyLock.unlock();
        }

    }


    private void acquireExclusive( Transaction transaction ) throws InterruptedException {
        if ( !isRoot() ) {
            parent.acquire( transaction, LockType.SHARED );
        }
        concurrencyLock.lock();
        try {
            while ( !owners.isEmpty() ) {
                DeadlockHandler.INSTANCE.addAndResolveDeadlock(this, transaction, owners.keySet() );
                concurrencyCondition.await();
            }
            isExclusive = true;
            owners.put( transaction, 1L );
            printInfo( "AEx", transaction );
        } finally {
            concurrencyLock.unlock();
        }
    }


    private boolean hasWaitingTransactions() {
        return concurrencyLock.hasWaiters( concurrencyCondition );
    }


    private void printInfo( String message, Transaction transaction ) {
        LOGGER.info ( MessageFormat.format(
                "{0}, TX: {1}, L: {2}",
                message,
                transaction,
                this
        ) );

    }
}
