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
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.DeadlockDetectorType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.util.DeadlockException;

public class DeadlockHandler {

    public static DeadlockHandler INSTANCE;

    private final DeadlockDetector deadlockDetector;
    private final DeadlockResolver deadlockResolver;

    private final ReentrantReadWriteLock concurrencyLock = new ReentrantReadWriteLock();
    private final Lock readLock = concurrencyLock.readLock();
    private final Lock writeLock = concurrencyLock.writeLock();

    static {
        DeadlockDetectorType deadlockDetectorType = (DeadlockDetectorType) RuntimeConfig.S2PL_DEADLOCK_DETECTOR_TYPE.getEnum();
        switch (deadlockDetectorType) {
            case GRAPH_DEADLOCK_DETECTOR ->
                    INSTANCE = new DeadlockHandler(new GraphDeadlockDetector(), new FirstTransactionDeadlockResolver());
            case SEQUENCE_DEADLOCK_DETECTOR ->
                    INSTANCE = new DeadlockHandler(new RequestSequenceDeadlockDetector(), new FirstTransactionDeadlockResolver());
            default ->
                    throw new DeadlockException("Illegal deadlock detector type: " + deadlockDetectorType.name());
        }
    }

    private DeadlockHandler(DeadlockDetector deadlockDetector, DeadlockResolver deadlockResolver) {
        this.deadlockDetector = deadlockDetector;
        this.deadlockResolver = deadlockResolver;
    }

    public void addAndResolveDeadlock(@NonNull Lockable lockable, @NonNull Transaction transaction, @NonNull Set<Transaction> owners) {
        writeLock.lock();
        try {
            deadlockDetector.add(lockable, transaction, owners);
            List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
            while (!conflictingTransactions.isEmpty()) {
                deadlockResolver.resolveDeadlock(conflictingTransactions);
                conflictingTransactions = deadlockDetector.getConflictingTransactions();
            }
        } finally {
            writeLock.unlock();
        }
    }


    public void remove(@NonNull Lockable lockable, @NonNull Transaction transaction ) {
        readLock.lock();
        try {
            deadlockDetector.remove(lockable, transaction );
        } finally {
            readLock.unlock();
        }
    }

}
