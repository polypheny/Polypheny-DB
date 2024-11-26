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
    private boolean hasNext = true; // whether new tuples are still being inserted into the queue


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
        queue.put( value );
    }


    @NotNull
    @Override
    public Iterator<List<PolyValue>> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return hasNext || !queue.isEmpty();
            }


            @Override
            public List<PolyValue> next() throws PipeInterruptedException {
                try {
                    return queue.take();
                } catch ( InterruptedException e ) {
                    throw new PipeInterruptedException( e );
                }
            }
        };
    }


    @Override
    public void close() throws Exception {
        hasNext = false;
    }

}
