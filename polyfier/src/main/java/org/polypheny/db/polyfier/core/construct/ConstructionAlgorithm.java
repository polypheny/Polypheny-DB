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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public abstract class ConstructionAlgorithm {


    public static class PolyTypeSubsetResolver<T extends Comparable<T>> {

        @Getter
        @AllArgsConstructor
        public static class ProjectionCodes {
            private final List<Integer> projectionCodeLeft;
            private final List<Integer> projectionCodeRight;
        }

        private ArrayList<ValWrapper<T>> xs;
        private ArrayList<ValWrapper<T>> ys;

        private ProjectionCodes projectionCodes;

        public PolyTypeSubsetResolver(final List<T> xs, final List<T> ys ) {
            if ( log.isDebugEnabled() ) {
                log.debug("-".repeat(120));
                log.debug("Solving Set-Projection Problem...");
            }

            this.xs = (ArrayList<ValWrapper<T>>) indexWrap( xs );
            this.ys = (ArrayList<ValWrapper<T>>) indexWrap( ys );

            removeDangling();

            this.projectionCodes = null;

            int k = 0;
            boolean flag = true;
            while ( ! this.xs.isEmpty() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( String.format("%-60s%-60s", this.xs.stream().map( ls -> ls.v.toString() + "#" + ls.idx ).collect(Collectors.toList()), this.ys.stream().map(ls ->  ls.v.toString() + "#" + ls.idx ).collect(Collectors.toList()) ));
                }
                if ( isSolution() ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Found Solution...");
                    }
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
            }

            if ( this.projectionCodes == null && log.isDebugEnabled() ) {
                log.debug("Found no Solution... But be not alarmed!");
            }
        }

        private void removeDangling() {
            this.xs = (ArrayList<ValWrapper<T>>) this.xs.stream().filter( this.ys::contains ).sorted().collect(Collectors.toList());
            Collections.reverse( this.xs );
            this.ys = (ArrayList<ValWrapper<T>>) this.ys.stream().filter( this.xs::contains ).sorted().collect(Collectors.toList());
            Collections.reverse( this.ys );
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
            return IntStream.range( 0, xs.size() ).mapToObj(k -> new ValWrapper<A>( xs.get( k ), k ) ).collect(Collectors.toList());
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


        public Optional<ProjectionCodes> solved() {
            if ( this.projectionCodes ==  null ) {
                return Optional.empty();
            }
            return Optional.of( projectionCodes );
        }

    }


}
