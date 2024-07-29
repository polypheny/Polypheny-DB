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

package org.polypheny.db.algebra;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;


/**
 * Tests for {@link AlgCollation} and {@link AlgFieldCollation}.
 */
public class AlgCollationTest {

    /**
     * Unit test for {@code RelCollations#contains}.
     */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testCollationContains() {
        final AlgCollation collation21 = AlgCollations.of( new AlgFieldCollation( 2, AlgFieldCollation.Direction.ASCENDING ), new AlgFieldCollation( 1, AlgFieldCollation.Direction.DESCENDING ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2 ) ), is( true ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 1 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 0 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2, 1 ) ), is( true ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2, 0 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2, 1, 3 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList() ), is( true ) );

        // if there are duplicates in keys, later occurrences are ignored
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2, 1, 2 ) ), is( true ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2, 1, 1 ) ), is( true ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 1, 2, 1 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 1, 1 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation21, Arrays.asList( 2, 2 ) ), is( true ) );

        final AlgCollation collation1 = AlgCollations.of( new AlgFieldCollation( 1, AlgFieldCollation.Direction.DESCENDING ) );
        assertThat( AlgCollations.contains( collation1, Arrays.asList( 1, 1 ) ), is( true ) );
        assertThat( AlgCollations.contains( collation1, Arrays.asList( 2, 2 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation1, Arrays.asList( 1, 2, 1 ) ), is( false ) );
        assertThat( AlgCollations.contains( collation1, Arrays.asList() ), is( true ) );
    }


    /**
     * Unit test for {@link AlgCollationImpl#compareTo}.
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


    private static AlgCollation collation( int... ordinals ) {
        final List<AlgFieldCollation> list = new ArrayList<>();
        for ( int ordinal : ordinals ) {
            list.add( new AlgFieldCollation( ordinal ) );
        }
        return AlgCollations.of( list );
    }

}

