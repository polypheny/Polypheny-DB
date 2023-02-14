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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PrecedenceClimbingParser;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Supplier;
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
            System.out.println( xs.toString() );
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


    public static class SetOpProblem<T extends Comparable<T>> implements Supplier<Optional<SetOpProblem.ProjectionCodes>> {

        @Getter
        @AllArgsConstructor
        public static class ProjectionCodes {
            private final List<Integer> projectionCodeLeft;
            private final List<Integer> projectionCodeRight;
        }

        private ArrayList<ValWrapper<T>> xs;
        private ArrayList<ValWrapper<T>> ys;

        private ProjectionCodes projectionCodes;

        public SetOpProblem(final List<T> xs, final List<T> ys ) {

            this.xs = (ArrayList<ValWrapper<T>>) indexWrap( xs );
            this.ys = (ArrayList<ValWrapper<T>>) indexWrap( ys );

            removeDangling();

            System.out.println( this.xs.toString() + "..." + this.ys.toString() );

            this.projectionCodes = null;

            int k = 0;
            boolean flag = true;
            while ( ! this.xs.isEmpty() ) {
                if ( isSolution() ) {
                    System.out.println( "Found Solution...");
                    this.projectionCodes = getSolution();
                    break;
                }
                k++;
                if ( k > 20 ) {
                    break;
                }

                Pair<Integer, Integer> c;
                if ( this.xs.size() > this.ys.size() ) {
                    for ( ValWrapper<T> x : this.xs ) {
                        c = count( x );

                        if ( c.left > c.right ) {
                            this.xs.remove( x );
                            break;
                        }

                    }
                } else if ( this.xs.size() < this.ys.size() ) {
                    for ( ValWrapper<T> x : this.ys ) {
                        c = count( x );

                        if ( c.right > c.left ) {
                            this.ys.remove( x );
                            break;
                        }

                    }
                } else {
                    if ( flag ) {
                        this.xs.remove( this.xs.size() - 1 );
                    } else {
                        this.ys.remove( this.ys.size() - 1 );
                    }
                    flag = ! flag;
                }


                System.out.println( this.xs.toString() + "..." + this.ys.toString() );
            }

            if ( this.projectionCodes == null ) {
                System.out.println("Found no Solution...");
            }
        }

        private void removeDangling() {
            this.xs = (ArrayList<ValWrapper<T>>) this.xs.stream().filter( this.ys::contains ).sorted().collect(Collectors.toList());
            this.ys = (ArrayList<ValWrapper<T>>) this.ys.stream().filter( this.xs::contains ).sorted().collect(Collectors.toList());
        }

        @Getter
        @AllArgsConstructor
        private static class ValWrapper<T extends Comparable<T>> implements Comparable<ValWrapper<T>> {
            private final T v;
            private final int idx;

            @Override
            public int compareTo(ValWrapper<T> o) {
                return getV().compareTo( o.getV() );
            }

            @Override
            public int hashCode() {
                return getV().hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                return obj instanceof ValWrapper && getV().equals( ((ValWrapper<?>) obj).getV() );
            }

            @Override
            public String toString() {
                return "( " + getIdx() + " : " + getV().toString() + " )";
            }

        }


        private static <A extends Comparable<A>> List<ValWrapper<A>> indexWrap( List<A> xs ) {
            return IntStream.range( 0, xs.size() ).mapToObj( k -> new ValWrapper<A>( xs.get( k ), k ) ).collect(Collectors.toList());
        }

        private Pair<Integer, Integer> count( ValWrapper<T> val ) {
            return Pair.of(
                    (int) xs.stream().filter( val::equals ).count(),
                    (int) ys.stream().filter( val::equals ).count()
            );
        }


        private ProjectionCodes getSolution() {
            return new ProjectionCodes(
                    this.xs.stream().mapToInt( ValWrapper::getIdx ).boxed().collect( Collectors.toList() ),
                    this.ys.stream().mapToInt( ValWrapper::getIdx ).boxed().collect( Collectors.toList() )
            );
        }

        private boolean isSolution() {
            if ( this.xs.size() != this.ys.size() ) {
                return false;
            }
            for ( int i = 0; i < this.xs.size(); i++ ) {
                if ( ! this.xs.get( i ).equals( this.ys.get( i ) ) ) {
                    return false;
                }
            }
            return true;
        }


        public Optional<ProjectionCodes> get() {
            if ( this.projectionCodes ==  null ) {
                return Optional.empty();
            }
            return Optional.of( projectionCodes );
        }



    }





}
