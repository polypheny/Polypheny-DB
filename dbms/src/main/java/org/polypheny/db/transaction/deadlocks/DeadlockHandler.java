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
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.DeadlockDetectorType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;

public class DeadlockHandler {

    public static final DeadlockHandler INSTANCE;

    private final DeadlockDetector deadlockDetector;

    private final ReentrantLock lock = new ReentrantLock( true );


    static {
        DeadlockDetectorType deadlockDetectorType = (DeadlockDetectorType) RuntimeConfig.S2PL_DEADLOCK_DETECTOR_TYPE.getEnum();
        DeadlockDetector detector = switch ( deadlockDetectorType ) {
            case GRAPH_DEADLOCK_DETECTOR -> new GraphDeadlockDetector();
        };

        INSTANCE = new DeadlockHandler( detector );
    }


    private DeadlockHandler( DeadlockDetector deadlockDetector ) {
        this.deadlockDetector = deadlockDetector;
    }


    public boolean addAndResolveDeadlock( @NonNull Lockable lockable, @NonNull Transaction transaction, @NonNull Set<Transaction> owners ) {
        lock.lock();
        try {
            deadlockDetector.add( lockable, transaction, owners );
            List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
            if ( !conflictingTransactions.isEmpty() ) {
                if ( !conflictingTransactions.contains( transaction ) ) {
                    throw new AssertionError( "Expected to be part of conflicting transactions" );
                }
                deadlockDetector.remove( lockable, transaction );
                return true;
            }
            return false;
//            return conflictingTransactions.contains( transaction );
//            if ( !conflictingTransactions.isEmpty() ) {
//                if ( !conflictingTransactions.contains( transaction ) ) {
//                    throw new AssertionError( "Expected to be part of conflicting transactions" );
//                }
//                return true;
//            }
//            return false;
        } finally {
            lock.unlock();
        }
    }


    public void remove( @NonNull Lockable lockable, @NonNull Transaction transaction ) {
        lock.lock();
        try {
            deadlockDetector.remove( lockable, transaction );
        } finally {
            lock.unlock();
        }
    }

}
