/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.util;


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
    Iterator<?> iterator;
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
