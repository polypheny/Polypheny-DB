/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import java.util.Iterator;


/**
 * Iterator that returns at most {@code limit} rows from an underlying {@link Iterator}.
 *
 * @param <E> element type
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
