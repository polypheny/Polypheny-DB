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

public interface InputPipe extends Iterable<List<PolyValue>> {

    AlgDataType getType();

    /**
     * For performance reasons, the returned iterator must be used carefully.
     * It is recommended to use a single enhanced for loop for iterating over all input values.
     * Please note the following restrictions:
     * <ul>
     *     <li>Only a single iterator can ever be returned.</li>
     *     <li>For graphs, the convention is that first all nodes are piped, followed by all edges.</li>
     *     <li>If the iterator is used directly, make sure to only call {@code next()} if {@code hasNext()} has been called immediately before and returned true.</li>
     *     <li>Calls to {@code hasNext()} must be separated by exactly one call to {@code next()}. Otherwise, you risk skipping over elements, as some implementations retrieve the actual next element during hasNext().</li>
     * </ul>
     */
    @NotNull
    @Override
    Iterator<List<PolyValue>> iterator();

    /**
     * A consumer can call this method to notify the producer that no more values need to be produced.
     */
    void finishIteration();

}
