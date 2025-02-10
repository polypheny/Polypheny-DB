package org.polypheny.db.transaction.deadlocks;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;
import lombok.NonNull;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;

public class RequestSequenceDeadlockDetector implements DeadlockDetector {

    private Transaction criticalTransaction = null;
    private final Map<Lockable, Long> lockableSequenceNumbers = new HashMap<>();

    @Override
    public synchronized void add(@NonNull Lockable lockable, @NonNull Transaction newTransaction, @NonNull Set<Transaction> owners) {
        long transactionSequenceNumber = newTransaction.getSequenceNumber();
        if (!lockableSequenceNumbers.containsKey(lockable)) {
            lockableSequenceNumbers.put(lockable, transactionSequenceNumber);
            return;
        }

        long existingSequenceNumber = lockableSequenceNumbers.get(lockable);

        if (transactionSequenceNumber > existingSequenceNumber) {
            criticalTransaction = newTransaction;
            return;
        }

        for (Transaction owner : owners) {
            lockableSequenceNumbers.put(lockable, Math.min(lockableSequenceNumbers.get(lockable), owner.getSequenceNumber()));
        }
    }

    @Override
    public synchronized List<Transaction> getConflictingTransactions() {
        if (criticalTransaction == null) {
            return List.of();
        }
        List<Transaction> conflictingTransactions = List.of(criticalTransaction);
        criticalTransaction = null;
        return conflictingTransactions;
    }

    @Override
    public synchronized void remove(@NonNull Lockable lockable, @NonNull Transaction transaction) {
        lockableSequenceNumbers.remove(lockable);
    }
}
