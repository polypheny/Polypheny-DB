package org.polypheny.db.transaction.deadlocks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.DeadlockDetectorType;
import org.polypheny.db.transaction.DeadlockResolverType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.util.DeadlockException;

public class DeadlockHandler {

    public static final DeadlockHandler INSTANCE;

    private final DeadlockDetector deadlockDetector;
    private final DeadlockResolver deadlockResolver;

    private final ReentrantLock lock = new ReentrantLock( true );


    static {
        DeadlockDetectorType deadlockDetectorType = (DeadlockDetectorType) RuntimeConfig.S2PL_DEADLOCK_DETECTOR_TYPE.getEnum();
        DeadlockDetector detector = switch ( deadlockDetectorType ) {
            case GRAPH_DEADLOCK_DETECTOR -> new GraphDeadlockDetector();
        };

        DeadlockResolverType deadlockResolverType = (DeadlockResolverType) RuntimeConfig.S2PL_DEADLOCK_RESOLVER_TYPE.getEnum();
        DeadlockResolver resolver = switch ( deadlockResolverType ) {
            case FIRST_TRANSACTION_DEADLOCK_RESOLVER -> new FirstTransactionDeadlockResolver();
            case LEAST_PROGRESS_DEADLOCK_RESOLVER -> new LeastProgressDeadlockResolver();
        };

        INSTANCE = new DeadlockHandler( detector, resolver );
    }


    private DeadlockHandler( DeadlockDetector deadlockDetector, DeadlockResolver deadlockResolver ) {
        this.deadlockDetector = deadlockDetector;
        this.deadlockResolver = deadlockResolver;
    }


    public void addAndResolveDeadlock( @NonNull Lockable lockable, @NonNull Transaction transaction, @NonNull Set<Transaction> owners ) throws InterruptedException {
        lock.lock();
        try {
            deadlockDetector.add( lockable, transaction, owners );
        } finally {
            lock.unlock();
        }

        List<Transaction> conflictingTransactions;
        while ( !(conflictingTransactions = deadlockDetector.getConflictingTransactions()).isEmpty() ) {
            lock.lock();
            try {
                deadlockResolver.resolveDeadlock( conflictingTransactions );
            } finally {
                lock.unlock();
            }

            if ( Thread.currentThread().isInterrupted() ) {
                throw new InterruptedException( "Transaction was interrupted due to deadlock resolution." );
            }
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
