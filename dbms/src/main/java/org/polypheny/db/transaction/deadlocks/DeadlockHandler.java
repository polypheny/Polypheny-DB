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

package org.polypheny.db.transaction.deadlocks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.NonNull;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;

public class DeadlockHandler {

    public static final DeadlockHandler INSTANCE = new DeadlockHandler( new RequestSequenceDeadlockDetector(), new FirstTransactionDeadlockResolver() );

    private final DeadlockDetector deadlockDetector;
    private final DeadlockResolver deadlockResolver;

    private final ReentrantReadWriteLock concurrencyLock = new ReentrantReadWriteLock();
    private final Lock sharedLock = concurrencyLock.readLock();
    private final Lock exclusiveLock = concurrencyLock.readLock();


    public DeadlockHandler(@NonNull DeadlockDetector deadlockDetector, @NonNull DeadlockResolver deadlockResolver ) {
        this.deadlockDetector = deadlockDetector;
        this.deadlockResolver = deadlockResolver;
    }


    public void addAndResolveDeadlock(@NonNull Lockable lockable, @NonNull Transaction transaction, @NonNull Set<Transaction> owners ) {
        sharedLock.lock();
        try {
            deadlockDetector.add(lockable, transaction, owners );
            exclusiveLock.lock();
            List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
            exclusiveLock.unlock();
            // lock can be release here as concurrently adding or removing transactions does not affect the resolution process
            while ( !conflictingTransactions.isEmpty() ) {
                deadlockResolver.resolveDeadlock( conflictingTransactions );
                exclusiveLock.lock();
                conflictingTransactions = deadlockDetector.getConflictingTransactions();
                exclusiveLock.unlock();
                // lock can be release here as concurrently adding or removing transactions does not affect the resolution process
            }
        } finally {
            sharedLock.unlock();
        }
    }


    public void remove(@NonNull Lockable lockable, @NonNull Transaction transaction ) {
        sharedLock.lock();
        try {
            deadlockDetector.remove(lockable, transaction );
        } finally {
            sharedLock.unlock();
        }
    }

}
