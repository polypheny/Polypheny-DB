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

package org.polypheny.db.runtime;

import com.drew.lang.annotations.NotNull;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.ExpressionTransformable;
import org.polypheny.db.util.BuiltInMethod;

public class PolyCollections {

    private static Gson gson = new GsonBuilder().serializeNulls().enableComplexMapKeySerialization().create();



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


        public static PolyDictionary fromString( String json ) {
            return gson.fromJson( json, PolyDictionary.class );
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
