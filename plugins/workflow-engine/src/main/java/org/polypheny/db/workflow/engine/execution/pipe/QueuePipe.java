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

package org.polypheny.db.workflow.engine.execution.pipe;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Pipeable.PipeInterruptedException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;

public class QueuePipe implements InputPipe, OutputPipe {

    private final BlockingQueue<List<PolyValue>> queue;
    private final AlgDataType type;
    private final ExecutionContext ctx;
    private boolean hasNext = true; // whether all tuples to ever be produced have been consumed
    private List<PolyValue> nextValue;
    private boolean startedIteration;

    private final long totalCount; // the estimated total number of tuples to be piped, or -1 if no estimation is possible
    private final long countDelta; // the number of tuples to be taken between updates to the progress;
    private final boolean canEstimateProgress;
    private long count;


    public QueuePipe( int capacity, AlgDataType type, ExecutionContext sourceCtx, long estimatedTotalCount ) {
        this.queue = new LinkedBlockingQueue<>( capacity );
        this.type = type;
        this.ctx = sourceCtx;
        this.totalCount = estimatedTotalCount;
        this.canEstimateProgress = totalCount > 0;
        this.countDelta = canEstimateProgress ? Math.max( totalCount / 100, 1 ) : -1;
    }


    @Override
    public AlgDataType getType() {
        return type;
    }


    @Override
    public void put( List<PolyValue> value ) throws InterruptedException {
        assert !value.isEmpty() : "Cannot pipe empty list, as it is used as an end marker.";
        queue.put( value );
        count++;
        if ( canEstimateProgress && count % countDelta == 0 ) {
            ctx.updateProgress( getEstimatedProgress() );
        }
    }


    @NotNull
    @Override
    public Iterator<List<PolyValue>> iterator() {
        if ( startedIteration ) {
            throw new IllegalStateException( "Cannot iterate more than once over the values of this pipe." );
        }
        startedIteration = true;
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if ( !hasNext ) {
                    return false;
                }
                try {
                    nextValue = queue.take();
                    if ( nextValue.isEmpty() ) {
                        hasNext = false;
                        return false;
                    }
                    return true;
                } catch ( InterruptedException e ) {
                    throw new PipeInterruptedException( e );
                }
            }


            @Override
            public List<PolyValue> next() throws PipeInterruptedException {
                assert hasNext;
                return nextValue; // important: hasNext must be called to load the next value
            }
        };
    }


    @Override
    public double getEstimatedProgress() {
        return canEstimateProgress ? (double) count / totalCount : -1;
    }


    @Override
    public void close() throws Exception {
        if ( hasNext ) {
            queue.put( List.of() ); // empty list is end marker
            // this could lead to multiple markers in the queue if closed more than once, but only the first one is relevant
        }
    }

}
