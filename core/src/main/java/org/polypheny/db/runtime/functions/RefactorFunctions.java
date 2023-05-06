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
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyString;
import org.polypheny.db.util.Pair;


public class RefactorFunctions {

    public static Object extractNames( Map<Object, Object> objects, List<String> names ) {
        Object[] ob = new Object[names.size() + 1];
        int i = 0;
        for ( String name : names ) {
            ob[i++] = objects.get( name );
        }
        names.forEach( objects::remove );
        ob[i] = new BsonDocument( objects.entrySet().stream().map( o -> new BsonElement( o.getKey().toString(), o.getValue() == null ? new BsonNull() : new BsonString( o.getValue().toString() ) ) ).collect( Collectors.toList() ) ).toJson();

        return ob;
    }


    public static PolyValue get( PolyDocument map, String key ) {
        return map.get( PolyString.of( key ) );
    }


    @SuppressWarnings("unused")
    public static PolyDocument removeNames( PolyDocument object, List<String> names ) {
        names.forEach( n -> object.remove( PolyString.of( n ) ) );
        return object;
    }


    @SuppressWarnings("unused")
    public static Object[] toObjectArray( Object... obj ) {
        return obj;
    }


    public static String fromDocument( PolyValue doc ) {
        return doc.serialize();
    }


    public static PolyValue toDocument( String json ) {
        return PolyValue.deserialize( json );
    }


    public static PolyDocument mergeDocuments( PolyValue target, Pair<String, PolyValue>... additional ) {
        for ( Pair<String, PolyValue> pair : additional ) {
            target.asDocument().put( new PolyString( pair.left ), pair.right );
        }

        return target.asDocument();
    }

}
