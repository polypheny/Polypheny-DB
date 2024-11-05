package org.polypheny.db.transaction;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.transaction.Lock.LockType;
import org.polypheny.db.util.ByteString;
import org.polypheny.db.util.DeadlockException;

public class LockTable {

    public static final LockTable INSTANCE = new LockTable();
    public static final ByteString GLOBAL_LOCK = ByteString.EMPTY;

    ConcurrentHashMap<ByteString, Lock> lockByEntry; // TODO TH: Currently we don't remove locks from this map as they might be requested again later. We might need a cleanup method for this later on.
    ConcurrentHashMap<Transaction, Set<Lock>> locksByTransaction;
    WaitForGraph waitForGraph;


    private LockTable() {
        lockByEntry = new ConcurrentHashMap<>();
        locksByTransaction = new ConcurrentHashMap<>();
        waitForGraph = new WaitForGraph();
    }


    public void lock( Transaction transaction, LockType lockType, ByteString entryId ) throws DeadlockException {
        lockByEntry.putIfAbsent( entryId, new Lock() );
        locksByTransaction.putIfAbsent( transaction, new HashSet<>() );

        Lock lock = lockByEntry.get( entryId );

        try {
            if ( !locksByTransaction.get( transaction ).contains( lock ) ) {
                lock.acquire( transaction, lockType, waitForGraph );
                locksByTransaction.get( transaction ).add( lock );
                return;
            }
            LockType heldLockType = lockByEntry.get( entryId ).getLockType();
            if ( heldLockType == lockType ) {
                return;
            }
            if ( heldLockType == LockType.SHARED ) {
                lock.upgradeToExclusive( transaction, waitForGraph );
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
                        lock.release(transaction, waitForGraph);
                    } catch (Exception e) {
                        throw new DeadlockException(MessageFormat.format("Failed to release lock for transaction {0}", transaction));
                    }
                }));
    }

}
