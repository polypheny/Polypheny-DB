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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import lombok.NonNull;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;

public class GraphDeadlockDetector implements DeadlockDetector {

    private final ConcurrentMap<Transaction, Set<Transaction>> adjacencyList = new ConcurrentHashMap<>();


    @Override
    public void add( @NonNull Lockable lockable, @NonNull Transaction newTransaction, @NonNull Set<Transaction> owners ) {
        Set<Transaction> currentSuccessors = adjacencyList.getOrDefault(
                newTransaction,
                new ConcurrentSkipListSet<>( Comparator.comparingLong( Transaction::getId ) )
        );
        currentSuccessors.addAll( owners );
        adjacencyList.put( newTransaction, currentSuccessors );
    }


    @Override
    public void remove( Lockable lockable, Transaction transaction ) {
        adjacencyList.remove( transaction );
        adjacencyList.forEach( ( predecessor, successors ) -> {
            if ( successors != null ) {
                successors.remove( transaction );
            }
        } );
    }


    @Override
    public List<Transaction> getConflictingTransactions() {
        Set<Transaction> visited = new HashSet<>();
        for ( Transaction startingNode : adjacencyList.keySet() ) {
            if ( visited.contains( startingNode ) ) {
                continue;
            }
            Set<Transaction> path = new HashSet<>();
            List<Transaction> cycle = detectCycle( startingNode, visited, path );
            if ( cycle != null ) {
                return cycle;
            }
        }
        return new ArrayList<>();
    }


    private List<Transaction> detectCycle( Transaction currentTransaction, Set<Transaction> visited, Set<Transaction> path ) {
        if ( path.contains( currentTransaction ) ) {
            return new ArrayList<>( path );
        }
        if ( visited.contains( currentTransaction ) ) {
            return null;
        }
        visited.add( currentTransaction );
        path.add( currentTransaction );
        if ( !adjacencyList.containsKey( currentTransaction ) ) {
            path.remove( currentTransaction );
            return null;
        }
        for ( Transaction neighbor : adjacencyList.get( currentTransaction ) ) {
            List<Transaction> cycle = detectCycle( neighbor, visited, path );
            if ( cycle != null ) {
                return cycle;
            }
        }
        path.remove( currentTransaction );
        return null;
    }

}
