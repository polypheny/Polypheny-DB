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

package org.polypheny.db.transaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Based on code taken from https://github.com/dstibrany/LockManager
public class WaitForGraph {

    private final ConcurrentMap<TransactionImpl, Set<TransactionImpl>> adjacencyList = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock sharedLock = rwl.readLock();
    private final Lock exclusiveLock = rwl.readLock();


    void add( TransactionImpl predecessor, Set<TransactionImpl> successors ) {
        sharedLock.lock();
        try {
            Set<TransactionImpl> txnList = adjacencyList.getOrDefault( predecessor, new ConcurrentSkipListSet<>() );
            txnList.addAll( successors );
            adjacencyList.put( predecessor, txnList );
        } finally {
            sharedLock.unlock();
        }
    }


    void remove( TransactionImpl txn ) {
        sharedLock.lock();
        try {
            adjacencyList.remove( txn );
            removeSuccessor( txn );
        } finally {
            sharedLock.unlock();
        }
    }


    boolean hasEdge( TransactionImpl txn1, TransactionImpl txn2 ) {
        Set<TransactionImpl> txnList = adjacencyList.get( txn1 );
        if ( txnList == null ) {
            return false;
        }
        return txnList.contains( txn2 );
    }


    List<List<TransactionImpl>> findCycles() {
        exclusiveLock.lock();
        try {
            DepthFirstSearch dfs = new DepthFirstSearch();
            dfs.start();
            return dfs.getCycles();
        } finally {
            exclusiveLock.unlock();
        }
    }


    void detectDeadlock( TransactionImpl currentTxn ) {
        List<List<TransactionImpl>> cycles = findCycles();

        for ( List<TransactionImpl> cycleGroup : cycles ) {
            if ( cycleGroup.contains( currentTxn ) ) {
                currentTxn.abort();
            }
        }
    }


    private void removeSuccessor( TransactionImpl txnToRemove ) {
        for ( TransactionImpl predecessor : adjacencyList.keySet() ) {
            Set<TransactionImpl> successors = adjacencyList.get( predecessor );
            if ( successors != null ) {
                successors.remove( txnToRemove );
            }
        }
    }


    class DepthFirstSearch {

        private final Set<TransactionImpl> visited = new HashSet<>();
        private final List<List<TransactionImpl>> cycles = new ArrayList<>();


        void start() {
            for ( TransactionImpl txn : adjacencyList.keySet() ) {
                if ( !visited.contains( txn ) ) {
                    visit( txn, new ArrayList<>() );
                }
            }
        }


        List<List<TransactionImpl>> getCycles() {
            return cycles;
        }


        Set<TransactionImpl> getVisited() {
            return visited;
        }


        private void visit( TransactionImpl node, List<TransactionImpl> path ) {
            visited.add( node );
            path.add( node );

            Set<TransactionImpl> set = adjacencyList.get( node );
            if ( set != null ) {
                for ( TransactionImpl neighbour : set ) {
                    if ( !visited.contains( neighbour ) ) {
                        visit( neighbour, new ArrayList<>( path ) );
                    } else {
                        if ( path.contains( neighbour ) ) {
                            cycles.add( getCycleFromPath( path, neighbour ) );
                        }
                    }
                }
            }
        }


        private List<TransactionImpl> getCycleFromPath( List<TransactionImpl> path, TransactionImpl target ) {
            return path.subList( path.indexOf( target ), path.size() );
        }
    }

}
