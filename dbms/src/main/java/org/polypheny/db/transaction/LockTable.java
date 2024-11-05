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

    ConcurrentHashMap<ByteString, Lock> locksByEntry; // TODO TH: Currently we don't remove locks from this list as they might be requested again later. We might need a cleanup method for this later on.
    ConcurrentHashMap<Transaction, Set<Lock>> locksByTransaction;


    private LockTable() {
        locksByEntry = new ConcurrentHashMap<>();
        locksByTransaction = new ConcurrentHashMap<>();
    }


    public void lock( Transaction transaction, LockType lockType, ByteString entryId ) throws DeadlockException {
        locksByEntry.putIfAbsent( entryId, new Lock() );
        locksByTransaction.putIfAbsent( transaction, new HashSet<>() );

        Lock lock = locksByEntry.get( entryId );

        try {
            if ( !locksByTransaction.get( transaction ).contains( lock ) ) {
                lock.aquire( lockType );
                locksByTransaction.get( transaction ).add( lock );
                return;
            }
            LockType heldLockType = locksByEntry.get( entryId ).getLockType();
            if ( heldLockType == lockType ) {
                return;
            }
            if ( heldLockType == LockType.SHARED ) {
                lock.upgradeToExclusive();
            }
        } catch ( InterruptedException e ) {
            unlockAll( transaction );
            throw new DeadlockException( MessageFormat.format( "Transaction {0} encountered a deadlock while acquiring a lock of type {1} on entry {2}.", transaction.getId(), lockType, entryId ) );
        }
    }

    public void unlockAll(Transaction transaction) throws DeadlockException {
        Optional.ofNullable(locksByTransaction.remove(transaction))
                .ifPresent(locks -> locks.forEach(Lock::release));
    }
}
