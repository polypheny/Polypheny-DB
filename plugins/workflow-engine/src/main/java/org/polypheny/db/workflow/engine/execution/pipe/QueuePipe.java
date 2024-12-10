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

public class QueuePipe implements InputPipe, OutputPipe {

    private final BlockingQueue<List<PolyValue>> queue;
    private final AlgDataType type;
    private boolean hasNext = true; // whether all tuples to ever be produced have been consumed
    private List<PolyValue> nextValue;


    public QueuePipe( int capacity, AlgDataType type ) {
        this.queue = new LinkedBlockingQueue<>( capacity );
        this.type = type;
    }


    @Override
    public AlgDataType getType() {
        return type;
    }


    @Override
    public void put( List<PolyValue> value ) throws InterruptedException {
        assert !value.isEmpty() : "Cannot pipe empty list, as it is used as an end marker.";
        queue.put( value );
    }


    @NotNull
    @Override
    public Iterator<List<PolyValue>> iterator() {
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
    public void close() throws Exception {
        if ( hasNext ) {
            queue.put( List.of() ); // empty list is end marker
            // this could lead to multiple markers in the queue if closed more than once, but only the first one is relevant
        }
    }

}
