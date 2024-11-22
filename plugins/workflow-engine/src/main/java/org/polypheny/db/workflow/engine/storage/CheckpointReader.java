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

import java.util.HashSet;
import java.util.Iterator;
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

    public abstract Iterator<PolyValue[]> getIterator();

    public abstract Iterator<PolyValue[]> getIteratorFromQuery( String query ); // TODO: How to specify query? Query language, PolyAlg or AlgNodes


    public AlgDataType getTupleType() {
        return entity.getTupleType();
    }


    void registerIterator( Iterator<PolyValue[]> iterator ) {
        if ( iterator instanceof AutoCloseable closeable ) {
            openIterators.add( closeable );
        }
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
