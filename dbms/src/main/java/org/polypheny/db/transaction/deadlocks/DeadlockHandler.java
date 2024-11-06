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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.polypheny.db.transaction.Transaction;

public class DeadlockHandler {

    private final DeadlockDetector deadlockDetector;
    private final DeadlockResolver deadlockResolver;

    private final ReentrantReadWriteLock concurrencyLock = new ReentrantReadWriteLock();
    private final Lock sharedLock = concurrencyLock.readLock();
    private final Lock exclusiveLock = concurrencyLock.readLock();


    public DeadlockHandler( DeadlockDetector deadlockDetector, DeadlockResolver deadlockResolver ) {
        this.deadlockDetector = deadlockDetector;
        this.deadlockResolver = deadlockResolver;
    }


    public void addAndResolveDeadlock( Transaction transaction, Set<Transaction> owners ) {
        sharedLock.lock();
        try {
            deadlockDetector.add(transaction, owners);
            exclusiveLock.lock();
            List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
            exclusiveLock.unlock();
            deadlockResolver.resolveDeadlock( conflictingTransactions );
        } finally {
            sharedLock.unlock();
        }
    }


    public void remove( Transaction transaction ) {
        sharedLock.lock();
        try {
            deadlockDetector.remove( transaction );
        } finally {
            sharedLock.unlock();
        }
    }

}
