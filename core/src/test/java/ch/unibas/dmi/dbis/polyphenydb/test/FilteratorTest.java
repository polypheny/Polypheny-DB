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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import ch.unibas.dmi.dbis.polyphenydb.util.Filterator;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;


/**
 * Unit test for {@link Filterator}.
 */
public class FilteratorTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testOne() {
        final List<String> tomDickHarry = Arrays.asList( "tom", "dick", "harry" );
        final Filterator<String> filterator = new Filterator<String>( tomDickHarry.iterator(), String.class );

        // call hasNext twice
        assertTrue( filterator.hasNext() );
        assertTrue( filterator.hasNext() );
        assertEquals( "tom", filterator.next() );

        // call next without calling hasNext
        assertEquals( "dick", filterator.next() );
        assertTrue( filterator.hasNext() );
        assertEquals( "harry", filterator.next() );
        assertFalse( filterator.hasNext() );
        assertFalse( filterator.hasNext() );
    }


    @Test
    public void testNulls() {
        // Nulls don't cause an error - but are not emitted, because they fail the instanceof test.
        final List<String> tomDickHarry = Arrays.asList( "paul", null, "ringo" );
        final Filterator<String> filterator = new Filterator<String>( tomDickHarry.iterator(), String.class );
        assertEquals( "paul", filterator.next() );
        assertEquals( "ringo", filterator.next() );
        assertFalse( filterator.hasNext() );
    }


    @Test
    public void testSubtypes() {
        final ArrayList arrayList = new ArrayList();
        final HashSet hashSet = new HashSet();
        final LinkedList linkedList = new LinkedList();
        Collection[] collections = { null, arrayList, hashSet, linkedList, null,
        };
        final Filterator<List> filterator =
                new Filterator<List>( Arrays.asList( collections ).iterator(), List.class );
        assertTrue( filterator.hasNext() );

        // skips null
        assertSame( arrayList, filterator.next() );

        // skips the HashSet
        assertSame( linkedList, filterator.next() );
        assertFalse( filterator.hasNext() );
    }


    @Test
    public void testBox() {
        final Number[] numbers = { 1, 2, 3.14, 4, null, 6E23 };
        List<Integer> result = new ArrayList<Integer>();
        for ( int i : Util.filter( Arrays.asList( numbers ), Integer.class ) ) {
            result.add( i );
        }
        assertEquals( "[1, 2, 4]", result.toString() );
    }
}
