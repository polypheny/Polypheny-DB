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

package org.polypheny.db.util.mapping;


import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import java.util.AbstractList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.runtime.Utilities;


/**
 * An immutable pair of integers.
 *
 * @see Mapping#iterator()
 */
public class IntPair {

    /**
     * Function that swaps source and target fields of an {@link IntPair}.
     */
    public static final Function<IntPair, IntPair> SWAP = pair -> of( pair.target, pair.source );

    /**
     * Ordering that compares pairs lexicographically: first by their source, then by their target.
     */
    public static final Ordering<IntPair> ORDERING = Ordering.from( Comparator.comparingInt( ( IntPair o ) -> o.source ).thenComparingInt( o -> o.target ) );

    /**
     * Function that returns the left (source) side of a pair.
     */
    public static final Function<IntPair, Integer> LEFT = pair -> pair.source;

    /**
     * Function that returns the right (target) side of a pair.
     */
    public static final Function<IntPair, Integer> RIGHT = pair -> pair.target;


    public final int source;
    public final int target;


    public IntPair( int source, int target ) {
        this.source = source;
        this.target = target;
    }


    public static IntPair of( int left, int right ) {
        return new IntPair( left, right );
    }


    public String toString() {
        return source + "-" + target;
    }


    public boolean equals( Object obj ) {
        if ( obj instanceof IntPair ) {
            IntPair that = (IntPair) obj;
            return (this.source == that.source) && (this.target == that.target);
        }
        return false;
    }


    public int hashCode() {
        return Utilities.hash( source, target );
    }


    /**
     * Converts two lists into a list of {@link IntPair}s, whose length is the lesser of the lengths of the source lists.
     *
     * @param lefts Left list
     * @param rights Right list
     * @return List of pairs
     */
    public static List<IntPair> zip( List<? extends Number> lefts, List<? extends Number> rights ) {
        return zip( lefts, rights, false );
    }


    /**
     * Converts two lists into a list of {@link IntPair}s.
     *
     * The length of the combined list is the lesser of the lengths of the source lists. But typically the source lists will be the same length.
     *
     * @param lefts Left list
     * @param rights Right list
     * @param strict Whether to fail if lists have different size
     * @return List of pairs
     */
    public static List<IntPair> zip( final List<? extends Number> lefts, final List<? extends Number> rights, boolean strict ) {
        final int size;
        if ( strict ) {
            if ( lefts.size() != rights.size() ) {
                throw new AssertionError();
            }
            size = lefts.size();
        } else {
            size = Math.min( lefts.size(), rights.size() );
        }
        return new AbstractList<IntPair>() {
            @Override
            public IntPair get( int index ) {
                return IntPair.of( lefts.get( index ).intValue(), rights.get( index ).intValue() );
            }


            @Override
            public int size() {
                return size;
            }
        };
    }


    /**
     * Returns the left side of a list of pairs.
     */
    public static List<Integer> left( final List<IntPair> pairs ) {
        return Lists.transform( pairs, LEFT );
    }


    /**
     * Returns the right side of a list of pairs.
     */
    public static List<Integer> right( final List<IntPair> pairs ) {
        return pairs.stream().map( RIGHT::apply ).collect( Collectors.toList() );
    }
}

