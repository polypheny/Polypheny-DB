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

package org.polypheny.db.runtime.functions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.function.Deterministic;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.polypheny.db.util.Pair;

@Deterministic
public interface RefactorFunctions {

    static Object extractNames( Map<Object, Object> objects, List<String> names ) {
        Object[] ob = new Object[names.size() + 1];
        int i = 0;
        for ( String name : names ) {
            ob[i++] = objects.get( name );
        }
        names.forEach( objects::remove );
        ob[i] = new BsonDocument( objects.entrySet().stream().map( o -> new BsonElement( o.getKey().toString(), o.getValue() == null ? new BsonNull() : new BsonString( o.getValue().toString() ) ) ).collect( Collectors.toList() ) ).toJson();

        return ob;
    }


    static <K, E> E get( Map<K, E> map, K key ) {
        return map.get( key );
    }


    @SuppressWarnings("unused")
    static Object removeNames( Map<String, Object> object, List<String> names ) {
        return object.entrySet().stream().filter( e -> names.contains( e.getKey() ) ).collect( Collectors.toList() );
    }


    @SuppressWarnings("unused")
    static Object[] toObjectArray( Object... obj ) {
        return obj;
    }

    static String fromDocument( Map<String, Object> doc ) {
        return BsonDocument.;
    }

    static Map<String, Object> toDocument( String json ) {
        return BsonDocument.parse( json );
    }


    static Map<String, Object> mergeDocuments( Map<String, Object> target, Pair<String, Map>... additional ) {
        for ( Pair<String, BsonDocument> pair : additional ) {
            target.put( pair.left, pair.right );
        }
        return target;
    }

}
