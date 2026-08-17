/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.execution;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Represents a single iterator that replays the buffered rows first
 * and then continues with the remaining original input rows.
 */
public class BufferedIterator<T, R> implements Iterator<R> {

    private final List<R> buffered;
    private final Iterator<T> remainder;
    private final Function<T, R> converter;
    private int index = 0;


    /**
     * Creates an instance of the iterator with the provided values.
     *
     * @param buffered - first sample rows used to infer schema (buffered)
     * @param remainder - remaining rows
     * @param converter -
     */
    public BufferedIterator( List<R> buffered, Iterator<T> remainder, Function<T, R> converter ) {
        this.buffered = buffered;
        this.remainder = remainder;
        this.converter = converter;
    }


    @Override
    public boolean hasNext() {
        return index < buffered.size() || remainder.hasNext();
    }


    @Override
    public R next() {
        if ( index < buffered.size() ) {
            return buffered.get( index++ );
        }
        return converter.apply( remainder.next() );
    }

}
