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

package org.polypheny.db.workflow.engine.execution.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;

public class QueueWrapper implements InputPipe, OutputPipe {

    private final BlockingQueue<PolyValue[]> queue;
    private final AlgDataType type;


    public QueueWrapper( int capacity, AlgDataType type ) {
        this.queue = new LinkedBlockingQueue<>( capacity );
        this.type = type;
    }


    @Override
    public PolyValue[] take() throws InterruptedException {
        return queue.take();
    }


    @Override
    public AlgDataType getType() {
        return type;
    }


    @Override
    public void put( PolyValue[] value ) throws InterruptedException {
        queue.put( value );
    }

}
