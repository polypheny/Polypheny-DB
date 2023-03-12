/*
 * Copyright 2019-2023 The Polypheny Project
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
 */

package org.polypheny.db.polyfier.core.construct;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

@Slf4j
public abstract class ConstructUtil {

    public static <A, B> List<Pair<A, B>> zip(List<A> xs, List<B> ys) {
        return IntStream.range(0, Math.min(xs.size(), ys.size()))
                .mapToObj(i -> new Pair<>(xs.get(i), ys.get(i)))
                .collect(Collectors.toList());
    }

    public static <A> List<A> flatten( List<List<A>> xs ) {
        if ( xs.contains( null )) {
            log.debug( xs.toString() );
        }

        List<A> ys = new LinkedList<>();
        xs.forEach( ys::addAll );
        return ys;
    }

    public static <A> List<Pair<A, A>> cross( List<A> xs ) {
        List<Pair<A, A>> cross = new LinkedList<>();
        LinkedList<A> var = new LinkedList<>(xs);
        for ( int i = 0; i < xs.size(); i++ ) {
            for ( int j = 0; j < xs.size(); j++ ) {
                cross.add( Pair.of( xs.get(  i ), var.get( j ) ) );
            }
            var.addLast( var.removeFirst() );
        }
        return cross;
    }

    public static <A> A choose( List<A> xs, Long seed ) {
        return xs.get( new Random( seed ).nextInt( xs.size() ) );
    }

    public static <A, B> List<Pair<A, B>> cross( List<A> xs, List<B> ys ) {
        List<Pair<A, B>> cross = new LinkedList<>();
        for (A xi : xs) {
            for (B yj: ys) {
                cross.add(Pair.of(xi, yj));
            }
        }
        return cross;
    }

    public static <A> List<Double> defaultWeights(List<A> xs ) {
        return DoubleStream
                        .generate( () -> 1.0d )
                        .limit( xs.size() )
                        .boxed()
                        .collect(Collectors.toList());
    }

    public static <A> List<List<A>> subsetFuzz( List<A> xs, Random rng, int fz, int max ) {
        List<List<A>> fuzz = new LinkedList<>();
        for ( int i = 0; i < fz; i++ ) {
            Set<A> set = new HashSet<>();
            for ( int j = 0; j < max; j++ ) {
                set.add( xs.get( rng.nextInt( 0, xs.size() ) ) );
            }
            fuzz.add( new LinkedList<>( set ) );
        }
        return fuzz;
    }


}
