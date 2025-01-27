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

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;

public interface OutputPipe extends AutoCloseable {

    /**
     * Puts the specified tuple into this pipe, possibly waiting for the pipe to have sufficient free capacity.
     * <p>
     * For graphs, the convention is that first all nodes are piped, followed by all edges.
     *
     * @param value the tuple to insert
     * @return false if any future value being put into this pipe is ignored. This allows the producer to prematurely stop in the case where the consumer
     * is not interested in any more values.
     * @throws InterruptedException if the thread was interrupted while waiting
     */
    boolean put( List<PolyValue> value ) throws InterruptedException;

    /**
     * Puts the specified value into this pipe, possibly waiting for the pipe to have sufficient free capacity.
     * <p>
     * For graphs, the convention is that first all nodes are piped, followed by all edges.
     *
     * @param value the single value to insert
     * @return false if any future value being put into this pipe is ignored. This allows the producer to prematurely stop in the case where the consumer
     * is not interested in any more values.
     * @throws InterruptedException if the thread was interrupted while waiting
     */
    default boolean put( PolyValue value ) throws InterruptedException {
        return put( List.of( value ) );
    }

    AlgDataType getType();


    /**
     * Returns an estimation for the proportion of all tuples already put into the pipe.
     * If no estimation is possible, -1 is returned.
     *
     * @return the estimated progress, guaranteed to be larger than 0 if estimation is possible. If the estimated total was too low, a progress larger than 1 might be returned.
     */
    double getEstimatedProgress();

}
