package org.polypheny.db.transaction.deadlocks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.DeadlockDetectorType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.util.DeadlockException;

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
                if ( conflictingTransactions.contains( transaction ) ) {
                    throw new AssertionError( "Expected to be part of conflicting transactions" );
                }
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
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
