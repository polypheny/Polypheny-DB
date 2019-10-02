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

package ch.unibas.dmi.dbis.polyphenydb.util;


import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Filtered iterator class: an iterator that includes only elements that are instanceof a specified class.
 *
 * @param <E> Element type
 * @see Util#cast(java.util.List, Class)
 * @see Util#cast(Iterator, Class)
 */
public class Filterator<E> implements Iterator<E> {

    Class<E> includeFilter;
    Iterator<? extends Object> iterator;
    E lookAhead;
    boolean ready;


    public Filterator( Iterator<?> iterator, Class<E> includeFilter ) {
        this.iterator = iterator;
        this.includeFilter = includeFilter;
    }


    @Override
    public boolean hasNext() {
        if ( ready ) {
            // Allow hasNext() to be called repeatedly.
            return true;
        }

        // look ahead to see if there are any additional elements
        try {
            lookAhead = next();
            ready = true;
            return true;
        } catch ( NoSuchElementException e ) {
            ready = false;
            return false;
        }
    }


    @Override
    public E next() {
        if ( ready ) {
            E o = lookAhead;
            ready = false;
            return o;
        }

        while ( iterator.hasNext() ) {
            Object o = iterator.next();
            if ( includeFilter.isInstance( o ) ) {
                return includeFilter.cast( o );
            }
        }
        throw new NoSuchElementException();
    }


    @Override
    public void remove() {
        iterator.remove();
    }
}

