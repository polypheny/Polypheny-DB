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


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.experimental.Delegate;
import lombok.experimental.NonFinal;
import org.apache.commons.compress.utils.Lists;
import org.jetbrains.annotations.NotNull;


/**
 * Space-efficient, comparable, immutable lists.
 */
@NonFinal // todo dl remove when own Immutable impls are completely removed...
public class ComparableList<T extends Comparable<T>> implements Comparable<ComparableList<T>>, List<T> {

    // used by BuiltInMethod
    public static final ComparableList<?> COMPARABLE_EMPTY_LIST = ComparableList.of();

    @Delegate
    List<T> list;


    public ComparableList( List<T> list ) {
        this.list = list;
    }


    @SafeVarargs
    public static <T extends Comparable<T>> ComparableList<T> of( T... elements ) {
        return new ComparableList<>( Arrays.asList( elements ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> of( Object e0 ) {
        return new ComparableList<>( Collections.singletonList( (T) e0 ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> of( Object e0, Object e1 ) {
        return new ComparableList<>( Arrays.asList( (T) e0, (T) e1 ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> of( Object e0, Object e1, Object e2 ) {
        return new ComparableList<>( Arrays.asList( (T) e0, (T) e1, (T) e2 ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> of( Object e0, Object e1, Object e2, Object e3 ) {
        return new ComparableList<>( Arrays.asList( (T) e0, (T) e1, (T) e2, (T) e3 ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> of( Object e0, Object e1, Object e2, Object e3, Object e4 ) {
        return new ComparableList<>( Arrays.asList( (T) e0, (T) e1, (T) e2, (T) e3, (T) e4 ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> of( Object e0, Object e1, Object e2, Object e3, Object e4, Object e5 ) {
        return new ComparableList<>( Arrays.asList( (T) e0, (T) e1, (T) e2, (T) e3, (T) e4, (T) e5 ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> copyOf( Iterator<? extends T> list ) {
        return new ComparableList<>( Lists.newArrayList( list ) );
    }


    public static <T extends Comparable<T>> ComparableList<T> copyOf( Collection<? extends T> list ) {
        return new ComparableList<>( List.copyOf( list ) );
    }


    @Override
    public int compareTo( @NotNull ComparableList<T> other ) {
        if ( size() != other.size() ) {
            return size() > other.size() ? 1 : -1;
        }
        for ( int i = 0; i < size(); i++ ) {
            Comparable<T> o0 = get( i );
            Comparable<T> o1 = other.get( i );
            int c = Objects.compare( o0, o1, ( a, b ) -> a.compareTo( (T) b ) );
            if ( c != 0 ) {
                return c;
            }
        }

        return 0;
    }


}

