/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.runtime;

import com.drew.lang.annotations.NotNull;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.ExpressionTransformable;
import org.polypheny.db.util.BuiltInMethod;

public class PolyCollections {

    public static class PolyList<T extends Comparable<?>> extends ArrayList<T> implements Comparable<PolyList<T>>, Collection<T>, ExpressionTransformable {


        public PolyList( Collection<T> list ) {
            super( list );
        }


        public PolyList() {
            super();
        }


        public static PolyList<?> of( ImmutableList<RexLiteral> operands ) {
            return new PolyList<>( operands );
        }


        public static PolyList<RexLiteral> ofArray( List<RexNode> operands ) {
            List<RexLiteral> ops = operands.stream().filter( o -> o.isA( Kind.LITERAL ) ).map( o -> (RexLiteral) o ).collect( Collectors.toList() );
            assert ops.size() == operands.size() : "Arrays are not nested for now.";

            return new PolyList<>( ops );
        }


        /**
         * If the size of left ( this ) is bigger return 1, if smaller return -1,
         * if sum of compared values of left is bigger than right return 1 and vice-versa
         */
        @Override
        public int compareTo( @NotNull PolyList<T> o ) {
            if ( this.size() != o.size() ) {
                return this.size() > o.size() ? 1 : 0;
            }

            long left = 0;
            long right = 0;
            int i = 0;
            int temp;
            for ( T t : this ) {
                temp = ((Comparable) t).compareTo( o.get( i ) );
                if ( temp < 0 ) {
                    right++;
                } else if ( temp > 0 ) {
                    left++;
                }
                i++;
            }

            return left == 0 && right == 0 ? 0 : left > right ? 1 : -1;
        }


        public static <T extends Comparable<T>> PolyList<T> of( Collection<T> list ) {
            return new PolyList<>( list );
        }


        @Override
        public Expression getAsExpression() {
            return EnumUtils.expressionList( stream().map( e -> EnumUtils.getExpression( e, Comparable.class ) ).collect( Collectors.toList() ) );
        }

    }


    public static class PolyDictionary extends HashMap<String, Object> implements Comparable<PolyDictionary>, ExpressionTransformable {


        public PolyDictionary( Map<String, Comparable<?>> map ) {
            super( map );
        }


        public PolyDictionary() {
            super();
        }


        @Override
        public Expression getAsExpression() {
            return Expressions.new_( PolyDictionary.class, Expressions.call(
                    BuiltInMethod.MAP_OF_ENTRIES.method,
                    EnumUtils.expressionList(
                            entrySet()
                                    .stream()
                                    .map( p -> Expressions.call(
                                            BuiltInMethod.PAIR_OF.method,
                                            Expressions.constant( p.getKey(), String.class ),
                                            EnumUtils.getExpression( p.getValue(), Object.class ) ) ).collect( Collectors.toList() ) ) ) );
        }


        @Override
        public int compareTo( @NotNull PolyDictionary directory ) {
            if ( size() > directory.size() ) {
                return 1;
            }
            if ( size() < directory.size() ) {
                return -1;
            }

            if ( this.equals( directory ) ) {
                return 0;
            }

            return hashCode() >= directory.hashCode() ? 1 : -1;

        }


    }


    public static class PolyMap<K extends Comparable<?>, V extends Comparable<?>> extends HashMap<K, V> implements Comparable<PolyMap<K, V>> {


        public PolyMap( Map<K, V> map ) {
            super( map );
        }


        public PolyMap() {
            super();
        }


        public static PolyMap<RexLiteral, RexLiteral> ofMap( Map<RexNode, RexNode> map ) {
            Map<RexLiteral, RexLiteral> checked = map.entrySet()
                    .stream()
                    .filter( e -> e.getKey().isA( Kind.LITERAL ) && e.getValue().isA( Kind.LITERAL ) )
                    .collect( Collectors.toMap( o -> (RexLiteral) o.getKey(), o -> (RexLiteral) o.getValue() ) );
            assert map.keySet().size() == checked.size() : "Map keys and values need to be RexLiterals";

            return PolyMap.of( checked );
        }


        public static <K extends Comparable<K>, V extends Comparable<V>> PolyMap<K, V> of( Map<K, V> map ) {
            return new PolyMap<>( map );
        }


        @Override
        public int compareTo( @NotNull PolyCollections.PolyMap<K, V> o ) {
            if ( this.size() != o.size() ) {
                return this.size() > o.size() ? 1 : 0;
            }

            int temp;
            for ( Entry<K, V> entry : this.entrySet() ) {
                if ( o.containsKey( entry.getKey() ) ) {
                    temp = ((Comparable) entry.getValue()).compareTo( o.get( entry.getKey() ) );

                    if ( temp != 0 ) {
                        return temp;
                    }
                } else {
                    return -1;
                }
            }
            return 0;
        }

    }

}
