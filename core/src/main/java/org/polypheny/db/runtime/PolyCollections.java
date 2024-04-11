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
 */

package org.polypheny.db.runtime;

import com.drew.lang.annotations.NotNull;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;

public class PolyCollections {

    private static Gson gson = new GsonBuilder().serializeNulls().enableComplexMapKeySerialization().create();


    public static class FlatMap<K extends Comparable<?>, V extends Comparable<?>> extends HashMap<K, V> implements Comparable<FlatMap<K, V>> {


        public FlatMap( Map<K, V> map ) {
            super( map );
        }


        public FlatMap() {
            super();
        }


        public static FlatMap<RexLiteral, RexLiteral> ofMap( Map<RexNode, RexNode> map ) {
            Map<RexLiteral, RexLiteral> checked = map.entrySet()
                    .stream()
                    .filter( e -> e.getKey().isA( Kind.LITERAL ) && e.getValue().isA( Kind.LITERAL ) )
                    .collect( Collectors.toMap( o -> (RexLiteral) o.getKey(), o -> (RexLiteral) o.getValue() ) );
            assert map.keySet().size() == checked.size() : "Map keys and values need to be RexLiterals";

            return FlatMap.of( checked );
        }


        public static <K extends Comparable<K>, V extends Comparable<V>> FlatMap<K, V> of( Map<K, V> map ) {
            return new FlatMap<>( map );
        }


        @Override
        public int compareTo( @NotNull FlatMap<K, V> o ) {
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
