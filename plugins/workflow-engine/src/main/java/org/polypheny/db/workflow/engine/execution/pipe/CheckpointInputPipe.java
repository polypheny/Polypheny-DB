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
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Pipeable.PipeInterruptedException;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

public class CheckpointInputPipe implements InputPipe, AutoCloseable {

    private final CheckpointReader reader;
    private final Iterator<List<PolyValue>> checkpointIterator;
    private final AlgDataType type;


    public CheckpointInputPipe( CheckpointReader reader ) {
        this.reader = reader;
        this.checkpointIterator = reader.getIterator();
        this.type = reader.getTupleType();
    }


    @Override
    public AlgDataType getType() {
        return type;
    }


    @NotNull
    @Override
    public Iterator<List<PolyValue>> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if ( Thread.interrupted() ) { // get the same behavior as with QueuePipe -> adds ability to interrupt execution
                    throw new PipeInterruptedException();
                }
                return checkpointIterator.hasNext();
            }


            @Override
            public List<PolyValue> next() {
                return checkpointIterator.next();
            }
        };
    }


    @Override
    public void close() throws Exception {
        try {
            reader.close();
        } catch ( Exception ignored ) {

        }
    }

}
