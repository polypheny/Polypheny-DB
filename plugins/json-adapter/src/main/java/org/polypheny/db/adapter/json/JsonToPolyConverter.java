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

package org.polypheny.db.adapter.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;

public final class JsonToPolyConverter {

    public PolyDocument nodeToPolyDocument( JsonNode node ) {
        return new PolyDocument( nodeToPolyMap( node ) );
    }


    private PolyMap<PolyString, PolyValue> nodeToPolyMap( JsonNode node ) {
        Map<PolyString, PolyValue> map = new HashMap<>();
        node.fields().forEachRemaining( entry -> {
            PolyString key = new PolyString( entry.getKey() );
            PolyValue value = nodeToPolyValue( entry.getValue() );
            map.put( key, value );
        } );
        return PolyMap.of( map );
    }


    public PolyValue nodeToPolyValue( JsonNode node ) {
        return switch ( node.getNodeType() ) {
            case NULL -> new PolyNull();
            case ARRAY -> nodeToPolyList( node );
            case OBJECT -> nodeToPolyMap( node );
            case NUMBER -> nodeToPolyNumber( node );
            case STRING -> new PolyString( node.asText() );
            case BOOLEAN -> new PolyBoolean( node.asBoolean() );
            case BINARY, MISSING, POJO -> new PolyNull();
        };
    }


    private PolyNumber nodeToPolyNumber( JsonNode node ) {
        if ( node.isIntegralNumber() ) {
            return new PolyLong( node.asLong() );
        }
        return new PolyDouble( node.asDouble() );
    }


    private PolyValue nodeToPolyList( JsonNode node ) {
        return PolyList.of( StreamSupport.stream( node.spliterator(), false )
                .map( this::nodeToPolyValue )
                .toList() );
    }

}
