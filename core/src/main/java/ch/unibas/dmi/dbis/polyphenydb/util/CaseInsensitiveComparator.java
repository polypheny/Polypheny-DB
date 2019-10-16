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


import java.util.Comparator;


/**
 * Comparator that compares all strings differently, but if two strings are equal in case-insensitive match they are right next to each other.
 *
 * Note: strings that differ only in upper-lower case are treated by this comparator as distinct.
 *
 * In a collection sorted on this comparator, we can find case-insensitive matches for a given string using
 * {@link #floorKey(java.lang.String)} and {@link #ceilingKey(java.lang.String)}.
 */
class CaseInsensitiveComparator implements Comparator {

    static final CaseInsensitiveComparator COMPARATOR = new CaseInsensitiveComparator();


    /**
     * Enables to create floor and ceiling keys for given string.
     */
    private static final class Key {

        public final String value;
        public final int compareResult;


        private Key( String value, int compareResult ) {
            this.value = value;
            this.compareResult = compareResult;
        }


        @Override
        public String toString() {
            return value;
        }
    }


    Object floorKey( String key ) {
        return new Key( key, -1 );
    }


    Object ceilingKey( String key ) {
        return new Key( key, 1 );
    }


    @Override
    public int compare( Object o1, Object o2 ) {
        String s1 = o1.toString();
        String s2 = o2.toString();
        int c = s1.compareToIgnoreCase( s2 );
        if ( c != 0 ) {
            return c;
        }
        if ( o1 instanceof Key ) {
            return ((Key) o1).compareResult;
        }
        if ( o2 instanceof Key ) {
            return -((Key) o2).compareResult;
        }
        return s1.compareTo( s2 );
    }
}
