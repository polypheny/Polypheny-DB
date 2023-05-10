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


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.function.Functions;
import org.polypheny.db.runtime.FlatList;
import org.polypheny.db.util.mapping.Mappings;


/**
 * An immutable list of {@link Integer} values backed by an array of {@code int}s.
 */
public class ImmutableIntList extends FlatList<Integer> {

    private static final Object[] EMPTY_ARRAY = new Object[0];

    private static final ImmutableIntList EMPTY = new EmptyImmutableIntList();


    // Does not copy array. Must remain private.
    private ImmutableIntList( int... ints ) {
        super( Arrays.stream( ints ).boxed().collect( Collectors.toList() ) );
    }


    private ImmutableIntList( List<Integer> list ) {
        super( list );
    }


    /**
     * Returns an empty ImmutableIntList.
     */
    public static ImmutableIntList of() {
        return EMPTY;
    }


    /**
     * Creates an ImmutableIntList from an array of {@code int}.
     */
    public static ImmutableIntList of( int... ints ) {
        return new ImmutableIntList( ints.clone() );
    }


    /**
     * Creates an ImmutableIntList from an array of {@code Number}.
     */
    public static ImmutableIntList copyOf( Number... numbers ) {
        final int[] ints = new int[numbers.length];
        for ( int i = 0; i < ints.length; i++ ) {
            ints[i] = numbers[i].intValue();
        }
        return new ImmutableIntList( ints );
    }


    /**
     * Creates an ImmutableIntList from an iterable of {@link Number}.
     */
    public static ImmutableIntList copyOf( Iterable<? extends Number> list ) {
        if ( list instanceof ImmutableIntList ) {
            return (ImmutableIntList) list;
        }
        @SuppressWarnings("unchecked") final Collection<? extends Number> collection =
                list instanceof Collection
                        ? (Collection<? extends Number>) list
                        : Lists.newArrayList( list );
        return copyFromCollection( collection );
    }


    /**
     * Creates an ImmutableIntList from an iterator of {@link Number}.
     */
    public static ImmutableIntList copyOf( Iterator<? extends Number> list ) {
        return copyFromCollection( Lists.newArrayList( list ) );
    }


    private static ImmutableIntList copyFromCollection( Collection<? extends Number> list ) {
        final int[] ints = new int[list.size()];
        int i = 0;
        for ( Number number : list ) {
            ints[i++] = number.intValue();
        }
        return new ImmutableIntList( ints );
    }


    /**
     * Returns an array of {@code int}s with the same contents as this list.
     */
    public int[] toIntArray() {
        return stream().mapToInt( i -> i ).toArray();
    }


    /**
     * Returns a copy of this list with one element added.
     */
    public ImmutableIntList append( int element ) {
        add( element );
        return new ImmutableIntList( this );
    }


    /**
     * Returns a list that contains the values lower to upper - 1.
     *
     * For example, {@code range(1, 3)} contains [1, 2].
     */
    public static List<Integer> range( final int lower, final int upper ) {
        return Functions.generate( upper - lower,
                new IntFunction<Integer>() {
                    @Override
                    public Integer apply( int index ) {
                        return lower + index;
                    }

                } );
    }


    /**
     * Returns the identity list [0, ..., count - 1].
     *
     * @see Mappings#isIdentity(List, int)
     */
    public static ImmutableIntList identity( int count ) {
        final int[] integers = new int[count];
        for ( int i = 0; i < integers.length; i++ ) {
            integers[i] = i;
        }
        return new ImmutableIntList( integers );
    }


    /**
     * Returns a copy of this list with all of the given integers added.
     */
    public ImmutableIntList appendAll( Iterable<Integer> list ) {
        if ( list instanceof Collection && ((Collection<?>) list).isEmpty() ) {
            return this;
        }
        return ImmutableIntList.copyOf( Iterables.concat( this, list ) );
    }


    /**
     * Special sub-class of {@link ImmutableIntList} that is always empty and has only one instance.
     */
    private static class EmptyImmutableIntList extends ImmutableIntList {

    }

}

