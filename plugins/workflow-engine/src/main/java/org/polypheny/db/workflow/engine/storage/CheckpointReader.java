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

package org.polypheny.db.workflow.engine.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;

public abstract class CheckpointReader implements AutoCloseable {

    final LogicalEntity entity;
    final TransactionManager transactionManager;
    final Transaction transaction;
    private final Set<AutoCloseable> openIterators = new HashSet<>();


    public CheckpointReader( LogicalEntity entity, Transaction transaction ) {
        this.entity = entity;
        this.transactionManager = transaction.getTransactionManager();
        this.transaction = transaction;
    }


    public abstract AlgNode getAlgNode( AlgCluster cluster );

    /**
     * Get a new PolyValue[] iterator for this checkpoint.
     * The iterator is closed automatically when this reader is closed.
     *
     * @return a PolyValue[] iterator representing the tuples of this checkpoint.
     */
    public abstract Iterator<PolyValue[]> getArrayIterator();


    /**
     * Get a new iterator for this checkpoint that transforms the raw PolyValue array into a
     * mutable list.
     * The iterator is closed automatically when this reader is closed.
     *
     * @return An iterator that yields tuples as mutable lists.
     */
    public final Iterator<List<PolyValue>> getIterator() {
        return arrayToListIterator( getArrayIterator(), false );
    }


    /**
     * Get a new iterator for this checkpoint that transforms the raw PolyValue array into a
     * list.
     *
     * @param fixedSize whether the generated lists by the iterator should be of fixedSize (more efficient).
     * @return An iterator that yields tuples as lists.
     */
    public final Iterator<List<PolyValue>> getIterator( boolean fixedSize ) {
        return arrayToListIterator( getArrayIterator(), fixedSize );
    }


    public abstract Iterator<List<PolyValue>> getIteratorFromQuery( String query ); // TODO: How to specify query? Query language, PolyAlg or AlgNodes


    public AlgDataType getTupleType() {
        return entity.getTupleType();
    }


    void registerIterator( Iterator<PolyValue[]> iterator ) {
        if ( iterator instanceof AutoCloseable closeable ) {
            openIterators.add( closeable );
        }
    }


    Iterator<List<PolyValue>> arrayToListIterator( Iterator<PolyValue[]> iterator, boolean fixedSize ) {
        if ( fixedSize ) {
            return new Iterator<>() {
                private final Iterator<PolyValue[]> arrayIterator = iterator;


                @Override
                public boolean hasNext() {
                    return arrayIterator.hasNext();
                }


                @Override
                public List<PolyValue> next() {
                    return Arrays.asList( arrayIterator.next() );
                }
            };
        }
        return new Iterator<>() {
            private final Iterator<PolyValue[]> arrayIterator = iterator;


            @Override
            public boolean hasNext() {
                return arrayIterator.hasNext();
            }


            @Override
            public List<PolyValue> next() {
                return new ArrayList<>( Arrays.asList( arrayIterator.next() ) );
            }
        };

    }


    @Override
    public void close() {
        for ( AutoCloseable iterator : openIterators ) {
            try {
                iterator.close();
            } catch ( Exception ignored ) {
            }
        }
        openIterators.clear();
        //transaction.rollback( null );  // read-only transaction TODO: activate when transactions fixed
    }

}
