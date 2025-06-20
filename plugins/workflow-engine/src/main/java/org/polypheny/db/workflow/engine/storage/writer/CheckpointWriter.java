/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.engine.storage.writer;

import java.util.Iterator;
import java.util.List;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

public abstract class CheckpointWriter implements AutoCloseable {


    final LogicalEntity entity;
    final Transaction transaction;
    final CheckpointMetadata metadata;


    public CheckpointWriter( LogicalEntity entity, Transaction transaction, CheckpointMetadata metadata ) {
        this.entity = entity;
        this.transaction = transaction;
        this.metadata = metadata;
    }


    /**
     * Writes the given tuple to this checkpoint.
     * The tuple type of the checkpoint must be compatible with this tuple.
     * The list is guaranteed to remain unmodified.
     *
     * @param tuple the tuple to write to this checkpoint
     */
    public abstract void write( List<PolyValue> tuple );


    public final void write( Iterator<List<PolyValue>> iterator ) {
        while ( iterator.hasNext() ) {
            write( iterator.next() );
        }
    }


    /**
     * Write the content of the given iterator and use the ExecutionContext
     * to check for interruptions after every written tuple.
     */
    public final void write( Iterator<List<PolyValue>> iterator, ExecutionContext ctx ) throws ExecutorException {
        while ( iterator.hasNext() ) {
            write( iterator.next() );
            ctx.checkInterrupted();
        }
    }


    public final void write( Iterable<List<PolyValue>> iterable ) {
        for ( List<PolyValue> tuple : iterable ) {
            write( tuple );
        }
    }


    /**
     * Write the content of the given iterable and use the ExecutionContext
     * to check for interruptions after every written tuple.
     */
    public final void write( Iterable<List<PolyValue>> iterable, ExecutionContext ctx ) throws ExecutorException {
        for ( List<PolyValue> tuple : iterable ) {
            write( tuple );
            ctx.checkInterrupted();
        }
    }


    /**
     * Returns the number of tuples written so far.
     * After the writer gets closed, this method should return the same value
     * as if calling {@link CheckpointReader#getTupleCount()} on a Reader that reads the checkpoint.
     *
     * @return long representing the written tuple count.
     */
    public abstract long getWriteCount();


    @Override
    public void close() throws Exception {
        // even in case of an error we can commit, since the checkpoint will be dropped
        try {
            transaction.commit();
        } catch ( Exception e ) {
            transaction.rollback( null );
            throw e;
        } finally {
            metadata.close();
        }
    }


}
