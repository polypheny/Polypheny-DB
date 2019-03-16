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
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate.Group;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for
 * {@link Group#induce(ImmutableBitSet, List)}.
 */
public class InduceGroupingTypeTest {

    @Test
    public void testInduceGroupingType() {
        final ImmutableBitSet groupSet = ImmutableBitSet.of( 1, 2, 4, 5 );

        // SIMPLE
        List<ImmutableBitSet> groupSets = new ArrayList<>();
        groupSets.add( groupSet );
        Assert.assertEquals( Aggregate.Group.SIMPLE, Aggregate.Group.induce( groupSet, groupSets ) );

        // CUBE
        groupSets = ImmutableBitSet.ORDERING.sortedCopy( groupSet.powerSet() );
        assertEquals( Aggregate.Group.CUBE, Aggregate.Group.induce( groupSet, groupSets ) );

        // ROLLUP
        groupSets = new ArrayList<>();
        groupSets.add( ImmutableBitSet.of( 1, 2, 4, 5 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2, 4 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2 ) );
        groupSets.add( ImmutableBitSet.of( 1 ) );
        groupSets.add( ImmutableBitSet.of() );
        assertEquals( Aggregate.Group.ROLLUP, Aggregate.Group.induce( groupSet, groupSets ) );

        // OTHER
        groupSets = new ArrayList<>();
        groupSets.add( ImmutableBitSet.of( 1, 2, 4, 5 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2, 4 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2 ) );
        groupSets.add( ImmutableBitSet.of() );
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        groupSets.add( ImmutableBitSet.of( 1, 2, 4, 5 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2, 4 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2 ) );
        groupSets.add( ImmutableBitSet.of( 1 ) );
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        groupSets.add( ImmutableBitSet.of( 1, 2, 5 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2, 4 ) );
        groupSets.add( ImmutableBitSet.of( 1, 2 ) );
        groupSets.add( ImmutableBitSet.of( 1 ) );
        groupSets.add( ImmutableBitSet.of() );

        try {
            final Aggregate.Group x = Aggregate.Group.induce( groupSet, groupSets );
            fail( "expected error, got " + x );
        } catch ( IllegalArgumentException ignore ) {
            // ok
        }

        groupSets = ImmutableBitSet.ORDERING.sortedCopy( groupSets );
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        groupSets.add( ImmutableBitSet.of() );
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );
    }


    /**
     * Tests a singleton grouping set {2}, whose power set has only two elements, { {2}, {} }.
     */
    @Test
    public void testInduceGroupingType1() {
        final ImmutableBitSet groupSet = ImmutableBitSet.of( 2 );

        // Could be ROLLUP but we prefer CUBE
        List<ImmutableBitSet> groupSets = new ArrayList<>();
        groupSets.add( groupSet );
        groupSets.add( ImmutableBitSet.of() );
        assertEquals( Aggregate.Group.CUBE, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        groupSets.add( ImmutableBitSet.of() );
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        groupSets.add( groupSet );
        assertEquals( Aggregate.Group.SIMPLE, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );
    }


    @Test
    public void testInduceGroupingType0() {
        final ImmutableBitSet groupSet = ImmutableBitSet.of();

        // Could be CUBE or ROLLUP but we choose SIMPLE
        List<ImmutableBitSet> groupSets = new ArrayList<>();
        groupSets.add( groupSet );
        assertEquals( Aggregate.Group.SIMPLE, Aggregate.Group.induce( groupSet, groupSets ) );

        groupSets = new ArrayList<>();
        assertEquals( Aggregate.Group.OTHER, Aggregate.Group.induce( groupSet, groupSets ) );
    }
}

