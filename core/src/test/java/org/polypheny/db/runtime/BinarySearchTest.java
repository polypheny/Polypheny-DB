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

package org.polypheny.db.runtime;


import com.google.common.collect.Ordering;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * Tests {@link BinarySearch}.
 */
public class BinarySearchTest {

    private void search( int key, int lower, int upper, Integer... array ) {
        Assertions.assertEquals( lower, BinarySearch.lowerBound( array, key, Ordering.natural() ), "lower bound of " + key + " in " + Arrays.toString( array ) );
        Assertions.assertEquals( upper, BinarySearch.upperBound( array, key, Ordering.natural() ), "upper bound of " + key + " in " + Arrays.toString( array ) );
    }


    @Test
    public void testSimple() {
        search( 1, 0, 0, 1, 2, 3 );
        search( 2, 1, 1, 1, 2, 3 );
        search( 3, 2, 2, 1, 2, 3 );
    }


    @Test
    public void testRepeated() {
        search( 1, 0, 1, 1, 1, 2, 2, 3, 3 );
        search( 2, 2, 3, 1, 1, 2, 2, 3, 3 );
        search( 3, 4, 5, 1, 1, 2, 2, 3, 3 );
    }


    @Test
    public void testMissing() {
        search( 0, -1, -1, 1, 2, 4 );
        search( 3, 2, 1, 1, 2, 4 );
        search( 5, 3, 3, 1, 2, 4 );
    }


    @Test
    public void testEmpty() {
        search( 42, -1, -1 );
    }


    @Test
    public void testSingle() {
        search( 41, -1, -1, 42 );
        search( 42, 0, 0, 42 );
        search( 43, 1, 1, 42 );
    }


    @Test
    public void testAllTheSame() {
        search( 1, 0, 3, 1, 1, 1, 1 );
        search( 0, -1, -1, 1, 1, 1, 1 );
        search( 2, 4, 4, 1, 1, 1, 1 );
    }

}
