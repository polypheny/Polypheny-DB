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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lock.LockType;
import org.polypheny.db.transaction.deadlocks.DeadlockHandler;
import org.polypheny.db.transaction.deadlocks.FirstTransactionDeadlockResolver;
import org.polypheny.db.transaction.deadlocks.GraphDeadlockDetector;
import org.polypheny.db.util.ByteString;
import org.polypheny.db.util.DeadlockException;

public class LockTable {

    public static final LockTable INSTANCE = new LockTable();
    public static final ByteString GLOBAL_LOCK = ByteString.EMPTY;

    private final ConcurrentHashMap<ByteString, Lock> lockByEntry; // TODO TH: Currently we don't remove locks from this map as they might be requested again later. We might need a cleanup method for this later on.
    private final ConcurrentHashMap<Transaction, Set<Lock>> locksByTransaction;
    final DeadlockHandler deadlockHandler;


    private LockTable() {
        this.lockByEntry = new ConcurrentHashMap<>();
        this.locksByTransaction = new ConcurrentHashMap<>();
        this.deadlockHandler = new DeadlockHandler( new GraphDeadlockDetector(), new FirstTransactionDeadlockResolver() );
    }

    //TODO TH: accept interfaces such as entity or lock instead of byte string
    public void lock( Transaction transaction, LockType lockType, ByteString entryId ) throws DeadlockException {
        lockByEntry.putIfAbsent( entryId, new Lock() );
        locksByTransaction.putIfAbsent( transaction, new HashSet<>() );

        Lock lock = lockByEntry.get( entryId );

        try {
            if ( !locksByTransaction.get( transaction ).contains( lock ) ) {
                lock.acquire( transaction, lockType);
                locksByTransaction.get( transaction ).add( lock );
                return;
            }
            LockType heldLockType = lockByEntry.get( entryId ).getLockType();
            if ( heldLockType == lockType ) {
                return;
            }
            if ( heldLockType == LockType.SHARED ) {
                lock.upgradeToExclusive( transaction);
            }
        } catch ( InterruptedException e ) {
            unlockAll( transaction );
            throw new DeadlockException( MessageFormat.format( "Transaction {0} encountered a deadlock while acquiring a lock of type {1} on entry {2}.", transaction.getId(), lockType, entryId ) );
        }
    }


    public void unlockAll( Transaction transaction ) throws DeadlockException {
        Optional.ofNullable(locksByTransaction.remove(transaction))
                .ifPresent(locks -> locks.forEach(lock -> {
                    try {
                        lock.release(transaction);
                    } catch (Exception e) {
                        throw new DeadlockException(MessageFormat.format("Failed to release lock for transaction {0}", transaction));
                    }
                }));
    }

}
