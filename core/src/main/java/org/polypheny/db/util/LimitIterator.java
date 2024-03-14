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

package org.polypheny.db.util;


import java.util.Iterator;


/**
 * Iterator that returns at most {@code limit} rows from an underlying {@link Iterator}.
 *
 */
public class LimitIterator<E> implements Iterator<E> {

    private final Iterator<E> iterator;
    private final long limit;
    int i = 0;


    private LimitIterator( Iterator<E> iterator, long limit ) {
        this.iterator = iterator;
        this.limit = limit;
    }


    public static <E> Iterator<E> of( Iterator<E> iterator, long limit ) {
        if ( limit <= 0 ) {
            return iterator;
        }
        return new LimitIterator<>( iterator, limit );
    }


    @Override
    public boolean hasNext() {
        return iterator.hasNext() && i < limit;
    }


    @Override
    public E next() {
        ++i;
        return iterator.next();
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
