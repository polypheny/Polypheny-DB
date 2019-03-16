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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import java.util.AbstractList;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Abstract base class for {@link RexInputRef} and {@link RexLocalRef}.
 */
public abstract class RexSlot extends RexVariable {

    protected final int index;


    /**
     * Creates a slot.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     */
    protected RexSlot( String name, int index, RelDataType type ) {
        super( name, type );
        assert index >= 0;
        this.index = index;
    }


    public int getIndex() {
        return index;
    }


    /**
     * Thread-safe list that populates itself if you make a reference beyond the end of the list. Useful if you are using the same entries repeatedly.
     * Once populated, accesses are very efficient.
     */
    protected static class SelfPopulatingList extends CopyOnWriteArrayList<String> {

        private final String prefix;


        SelfPopulatingList( final String prefix, final int initialSize ) {
            super( fromTo( prefix, 0, initialSize ) );
            this.prefix = prefix;
        }


        private static AbstractList<String> fromTo( final String prefix, final int start, final int end ) {
            return new AbstractList<String>() {
                public String get( int index ) {
                    return prefix + (index + start);
                }


                public int size() {
                    return end - start;
                }
            };
        }


        @Override
        public String get( int index ) {
            for ( ; ; ) {
                try {
                    return super.get( index );
                } catch ( IndexOutOfBoundsException e ) {
                    if ( index < 0 ) {
                        throw new IllegalArgumentException();
                    }
                    // Double-checked locking, but safe because CopyOnWriteArrayList.array is marked volatile, and size() uses array.length.
                    synchronized ( this ) {
                        final int size = size();
                        if ( index >= size ) {
                            addAll( fromTo( prefix, size, Math.max( index + 1, size * 2 ) ) );
                        }
                    }
                }
            }
        }
    }
}

