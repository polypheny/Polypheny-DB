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

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import lombok.NonNull;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;

public class RequestSequenceDeadlockDetector implements DeadlockDetector {

    Transaction criticalTransaction = null;
    HashMap<Lockable, Long> lockableSequenceNumbers = new HashMap<>();
    HashMap<Transaction, Long> transactionSequenceNumbers = new HashMap<>();


    @Override
    public void add( @NonNull Lockable lockable, @NonNull Transaction newTransaction, @NonNull Set<Transaction> owners ) {
        long transactionSequenceNumber = transactionSequenceNumbers.getOrDefault( newTransaction, System.nanoTime() );
        if ( !lockableSequenceNumbers.containsKey( lockable ) ) {
            lockableSequenceNumbers.put( lockable, transactionSequenceNumber );
            transactionSequenceNumbers.put( newTransaction, transactionSequenceNumber );
            return;
        }
        long lockableSequenceNumber = lockableSequenceNumbers.get( lockable );
        if ( transactionSequenceNumber > lockableSequenceNumber ) {
            criticalTransaction = newTransaction;
            return;
        }
        transactionSequenceNumbers.put( newTransaction, transactionSequenceNumber );
    }


    @Override
    public List<Transaction> getConflictingTransactions() {
        if ( criticalTransaction == null ) {
            return List.of();
        }
        List<Transaction> conflictingTransactions = List.of( criticalTransaction );
        criticalTransaction = null;
        return conflictingTransactions;
    }


    @Override
    public void remove( @NonNull Lockable lockable, @NonNull Transaction transaction ) {
        lockableSequenceNumbers.remove( lockable );
        transactionSequenceNumbers.remove( transaction );
    }

}
