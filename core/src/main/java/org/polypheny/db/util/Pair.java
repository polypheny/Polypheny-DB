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

package org.polypheny.db.util;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.util.Pair.PairSerializer;


/**
 * Wrapper object for a pair of objects.
 * <p>
 * Because a pair implements {@link #equals(Object)} and {@link #hashCode()}, it can be used in any kind of {@link java.util.Collection}.
 *
 * @param <T1> Left-hand type
 * @param <T2> Right-hand type
 */
@Value
@NonFinal
@JsonSerialize(using = PairSerializer.class)
public class Pair<T1, T2> implements Map.Entry<T1, T2>, Serializable {

    @Serialize
    @JsonProperty
    public T1 left;

    @Serialize
    @JsonProperty
    public T2 right;


    /**
     * Creates a Pair.
     *
     * @param left left value
     * @param right right value
     */
    public Pair(
            @Deserialize("left") T1 left,
            @Deserialize("right") T2 right ) {
        this.left = left;
        this.right = right;
    }


    /**
     * Creates a Pair of appropriate type.
     * <p>
     * This is a shorthand that allows you to omit implicit types. For example, you can write:
     * <blockquote>return Pair.of(s, n);</blockquote>
     * instead of
     * <blockquote>return new Pair&lt;String, Integer&gt;(s, n);</blockquote>
     *
     * @param left left value
     * @param right right value
     * @return A Pair
     */
    public static <T1, T2> Pair<T1, T2> of( T1 left, T2 right ) {
        return new Pair<>( left, right );
    }


    /**
     * Creates a {@code Pair} from a {@link java.util.Map.Entry}.
     */
    public static <K, V> Pair<K, V> of( Map.Entry<K, V> entry ) {
        return of( entry.getKey(), entry.getValue() );
    }


    public boolean equals( Object obj ) {
        return this == obj
                || (obj instanceof Pair)
                && Objects.equals( this.left, ((Pair<?, ?>) obj).left )
                && Objects.equals( this.right, ((Pair<?, ?>) obj).right );
    }


    /**
     * {@inheritDoc}
     *
     * Computes hash code consistent with {@link java.util.Map.Entry#hashCode()}.
     */
    @Override
    public int hashCode() {
        int keyHash = left == null ? 0 : left.hashCode();
        int valueHash = right == null ? 0 : right.hashCode();
        return keyHash ^ valueHash;
    }


    public String toString() {
        return "<" + left + ", " + right + ">";
    }


    @Override
    public T1 getKey() {
        return left;
    }


    @Override
    public T2 getValue() {
        return right;
    }


    @Override
    public T2 setValue( T2 value ) {
        throw new UnsupportedOperationException();
    }



    /**
     * Converts two lists into a list of {@link Pair}s, whose length is the lesser of the lengths of the source lists.
     *
     * @param ks Left list
     * @param vs Right list
     * @return List of pairs
     * @see org.apache.calcite.linq4j.Ord#zip(java.util.List)
     */
    public static <K, V> List<Pair<K, V>> zip( List<K> ks, List<V> vs ) {
        return zip( ks, vs, false );
    }


    /**
     * Converts two lists into a list of {@link Pair}s.
     * <p>
     * The length of the combined list is the lesser of the lengths of the source lists. But typically the source lists will be the same length.
     *
     * @param ks Left list
     * @param vs Right list
     * @param strict Whether to fail if lists have different size
     * @return List of pairs
     * @see org.apache.calcite.linq4j.Ord#zip(java.util.List)
     */
    public static <K, V> List<Pair<K, V>> zip( final List<K> ks, final List<V> vs, boolean strict ) {
        final int size;
        if ( strict ) {
            if ( ks.size() != vs.size() ) {
                throw new AssertionError();
            }
            size = ks.size();
        } else {
            size = Math.min( ks.size(), vs.size() );
        }
        return new AbstractList<>() {
            @Override
            public Pair<K, V> get( int index ) {
                return Pair.of( ks.get( index ), vs.get( index ) );
            }


            @Override
            public int size() {
                return size;
            }
        };
    }


    /**
     * Converts two iterables into an iterable of {@link Pair}s.
     * <p>
     * The resulting iterator ends whenever the first of the input iterators ends. But typically the source iterators will be the same length.
     *
     * @param ks Left iterable
     * @param vs Right iterable
     * @return Iterable over pairs
     */
    public static <K, V> Iterable<Pair<K, V>> zip( final Iterable<? extends K> ks, final Iterable<? extends V> vs ) {
        return () -> {
            final Iterator<? extends K> kIterator = ks.iterator();
            final Iterator<? extends V> vIterator = vs.iterator();

            return new ZipIterator<>( kIterator, vIterator );
        };
    }


    public static <K, V> List<K> left( final List<? extends Map.Entry<K, V>> pairs ) {
        return new AbstractList<>() {
            @Override
            public K get( int index ) {
                return pairs.get( index ).getKey();
            }


            @Override
            public int size() {
                return pairs.size();
            }
        };
    }


    public static <K, V> List<V> right( final List<? extends Map.Entry<K, V>> pairs ) {
        return new AbstractList<>() {
            @Override
            public V get( int index ) {
                return pairs.get( index ).getValue();
            }


            @Override
            public int size() {
                return pairs.size();
            }
        };
    }


    /**
     * Iterator that pairs elements from two iterators.
     *
     * @param <L> Left-hand type
     * @param <R> Right-hand type
     */
    private record ZipIterator<L, R>( Iterator<? extends L> leftIterator, Iterator<? extends R> rightIterator ) implements Iterator<Pair<L, R>> {

        private ZipIterator( Iterator<? extends L> leftIterator, Iterator<? extends R> rightIterator ) {
            this.leftIterator = Objects.requireNonNull( leftIterator );
            this.rightIterator = Objects.requireNonNull( rightIterator );
        }


        @Override
        public boolean hasNext() {
            return leftIterator.hasNext() && rightIterator.hasNext();
        }


        @Override
        public Pair<L, R> next() {
            return Pair.of( leftIterator.next(), rightIterator.next() );
        }


        @Override
        public void remove() {
            leftIterator.remove();
            rightIterator.remove();
        }

    }


    static class PairSerializer extends JsonSerializer<Pair<?, ?>> {

        @Override
        public void serialize( Pair<?, ?> value, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeStartObject();
            gen.writeObjectField( "left", value.left );
            gen.writeObjectField( "right", value.right );
            gen.writeEndObject();
        }

    }

}

