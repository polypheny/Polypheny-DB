/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.runtime;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import javax.annotation.Nonnull;


/**
 * Base class for lists whose contents are constant after creation.
 *
 * @param <E> Element type
 */
abstract class AbstractImmutableList<E> implements List<E> {

    protected abstract List<E> toList();


    @Override
    @Nonnull
    public Iterator<E> iterator() {
        return toList().iterator();
    }


    @Override
    @Nonnull
    public ListIterator<E> listIterator() {
        return toList().listIterator();
    }


    @Override
    public boolean isEmpty() {
        return false;
    }


    @Override
    public boolean add( E t ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean addAll( @Nonnull Collection<? extends E> c ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean addAll( int index, @Nonnull Collection<? extends E> c ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean removeAll( @Nonnull Collection<?> c ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean retainAll( @Nonnull Collection<?> c ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }


    @Override
    public E set( int index, E element ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void add( int index, E element ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public E remove( int index ) {
        throw new UnsupportedOperationException();
    }


    @Override
    @Nonnull
    public ListIterator<E> listIterator( int index ) {
        return toList().listIterator( index );
    }


    @Override
    @Nonnull
    public List<E> subList( int fromIndex, int toIndex ) {
        return toList().subList( fromIndex, toIndex );
    }


    @Override
    public boolean contains( Object o ) {
        return indexOf( o ) >= 0;
    }


    @Override
    public boolean containsAll( @Nonnull Collection<?> c ) {
        for ( Object o : c ) {
            if ( !contains( o ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean remove( Object o ) {
        throw new UnsupportedOperationException();
    }
}

