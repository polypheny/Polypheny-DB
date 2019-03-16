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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;


/**
 * Tests for {@link RelCollation} and {@link RelFieldCollation}.
 */
public class RelCollationTest {

    /**
     * Unit test for {@link RelCollations#contains}.
     */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testCollationContains() {
        final RelCollation collation21 = RelCollations.of( new RelFieldCollation( 2, RelFieldCollation.Direction.ASCENDING ), new RelFieldCollation( 1, RelFieldCollation.Direction.DESCENDING ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2 ) ), is( true ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 1 ) ), is( false ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 0 ) ), is( false ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2, 1 ) ), is( true ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2, 0 ) ), is( false ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2, 1, 3 ) ), is( false ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList() ), is( true ) );

        // if there are duplicates in keys, later occurrences are ignored
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2, 1, 2 ) ), is( true ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2, 1, 1 ) ), is( true ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 1, 2, 1 ) ), is( false ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 1, 1 ) ), is( false ) );
        assertThat( RelCollations.contains( collation21, Arrays.asList( 2, 2 ) ), is( true ) );

        final RelCollation collation1 = RelCollations.of( new RelFieldCollation( 1, RelFieldCollation.Direction.DESCENDING ) );
        assertThat( RelCollations.contains( collation1, Arrays.asList( 1, 1 ) ), is( true ) );
        assertThat( RelCollations.contains( collation1, Arrays.asList( 2, 2 ) ), is( false ) );
        assertThat( RelCollations.contains( collation1, Arrays.asList( 1, 2, 1 ) ), is( false ) );
        assertThat( RelCollations.contains( collation1, Arrays.asList() ), is( true ) );
    }


    /**
     * Unit test for {@link RelCollationImpl#compareTo}.
     */
    @Test
    public void testCollationCompare() {
        assertThat( collation( 1, 2 ).compareTo( collation( 1, 2 ) ), equalTo( 0 ) );
        assertThat( collation( 1, 2 ).compareTo( collation( 1 ) ), equalTo( 1 ) );
        assertThat( collation( 1 ).compareTo( collation( 1, 2 ) ), equalTo( -1 ) );
        assertThat( collation( 1, 3 ).compareTo( collation( 1, 2 ) ), equalTo( 1 ) );
        assertThat( collation( 0, 3 ).compareTo( collation( 1, 2 ) ), equalTo( -1 ) );
        assertThat( collation().compareTo( collation( 0 ) ), equalTo( -1 ) );
        assertThat( collation( 1 ).compareTo( collation() ), equalTo( 1 ) );
    }


    private static RelCollation collation( int... ordinals ) {
        final List<RelFieldCollation> list = new ArrayList<>();
        for ( int ordinal : ordinals ) {
            list.add( new RelFieldCollation( ordinal ) );
        }
        return RelCollations.of( list );
    }
}

