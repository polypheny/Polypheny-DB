/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb.bson;

import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;

public final class MongoDocumentSerializer {

    private MongoDocumentSerializer() {
    }


    public static BsonDocument serialize( PolyDocument document ) {
        return BsonDocument.parse( toExtendedJson( document ) );
    }


    public static BsonValue serializeValue( PolyValue value ) {
        return BsonDocument.parse( "{\"value\":" + toExtendedJson( value ) + "}" ).get( "value" );
    }


    public static String toExtendedJson( PolyValue value ) {
        if ( value == null ) {
            return "null";
        }
        if ( value instanceof PolyBigDecimal decimal && !decimal.isNull() ) {
            // A bare JSON number would be parsed as a double or a signed 64-bit integer.
            return "{\"$numberDecimal\":\"" + decimal.value + "\"}";
        }
        if ( value instanceof PolyLong number && number.value != null ) {
            return "{\"$numberLong\":\"" + number.value + "\"}";
        }
        if ( value instanceof PolyMap<?, ?> map && map.map != null ) {
            return map.entrySet().stream().map( entry -> {
                PolyValue key = entry.getKey();
                String field = key.isString() ? key.asString().toQuotedJson() : key.toJson();
                return field + ":" + toExtendedJson( entry.getValue() );
            } ).collect( Collectors.joining( ",", "{", "}" ) );
        }
        if ( value instanceof PolyList<?> list && list.value != null ) {
            return list.stream().map( MongoDocumentSerializer::toExtendedJson ).collect( Collectors.joining( ",", "[", "]" ) );
        }
        if ( value.isString() ) {
            return value.asString().toQuotedJson();
        }
        if ( value.isDate() || value.isTime() || value.isTimestamp() ) {
            return "\"" + value.toJson() + "\"";
        }
        return value.toJson();
    }

}
