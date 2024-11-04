package org.polypheny.db.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.polypheny.db.transaction.Lock.LockType;
import org.polypheny.db.util.ByteString;

public class LockTable {

    public static final LockTable INSTANCE = new LockTable();
    public static final ByteString GLOBAL_ENTRY_ID = ByteString.EMPTY;

    private final Map<ByteString, Lock> locks;
    private final Map<ByteString, Queue<Transaction>> waitingQueues;
    private final Map<Transaction, Set<ByteString>> entriesByTransaction;
    private final ReentrantLock lockTable;
    private final Condition waitingCondition;

    private LockTable() {
        locks = new HashMap<>();
        waitingQueues = new HashMap<>();
        this.entriesByTransaction = new HashMap<>();
        lockTable = new ReentrantLock();
        waitingCondition = lockTable.newCondition();
    }

    public void lock(Transaction transaction, LockType lockType, ByteString entryId) throws InterruptedException {
        lockTable.lock();
        try {
            while (true) {
                Lock currentLock = locks.get(entryId);
                if (currentLock == null) {
                    locks.put(entryId, new Lock(transaction, lockType, entryId));
                    entriesByTransaction.computeIfAbsent(transaction, k -> new HashSet<>()).add(entryId);
                    return;
                }
                if (currentLock.getLockType() == LockType.SHARED && lockType == LockType.SHARED) {
                    currentLock.getOwners().add(transaction);
                    entriesByTransaction.computeIfAbsent(transaction, k -> new HashSet<>()).add(entryId);
                    return;
                }
                if (currentLock.getOwners().contains(transaction) && currentLock.getLockType() == lockType) {
                    return;
                }
                waitingQueues.computeIfAbsent(entryId, k -> new LinkedList<>()).add(transaction);
                waitingCondition.await();
            }
        } finally {
            lockTable.unlock();
        }
    }

    public void unlockAll(Transaction transaction) {
        lockTable.lock();
        try {
            Set<ByteString> entries = entriesByTransaction.get(transaction);
            if (entries == null) {
                return;
            }
            entries.forEach(entryId -> unlockUnsave(transaction, entryId));
            entriesByTransaction.remove(transaction);
        } finally {
            lockTable.unlock();
        }
    }

    private void unlockUnsave(Transaction transaction, ByteString entryId) {
        Lock currentLock = locks.get(entryId);
        if (currentLock == null) {
            return;
        }
        if (currentLock.getOwners().remove(transaction)) {
            locks.remove(entryId);
            removeLockIfNoOwnerUnsave(currentLock, entryId);
        }
    }

    private void removeLockIfNoOwnerUnsave(Lock lock, ByteString entryId) {
        if (!lock.getOwners().isEmpty()) {
            return;
        }
        locks.remove(entryId);
        waitingCondition.signalAll();
    }
}
