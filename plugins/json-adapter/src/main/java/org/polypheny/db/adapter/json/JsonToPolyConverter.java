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
import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Collectors;
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

public class JsonToPolyConverter {

    public PolyDocument nodeToPolyDocument( JsonNode node ) {
        return new PolyDocument( nodeToPolyMap( node ) );
    }


    public PolyMap<PolyString, PolyValue> nodeToPolyMap( JsonNode node ) {
        HashMap<PolyString, PolyValue> map = new HashMap<>();
        node.fields().forEachRemaining( entry -> {
            PolyString key = new PolyString( entry.getKey() );
            PolyValue value = nodeToPolyValue( entry.getValue() );
            map.put( key, value );
        } );
        return PolyMap.of( map );
    }


    public PolyValue nodeToPolyValue( JsonNode node ) {
        switch ( node.getNodeType() ) {
            case NULL -> {return nodeToPolyNull();}
            case ARRAY -> {return nodeToPolyList( node );}
            case OBJECT -> {return nodeToPolyMap( node );}
            case NUMBER -> {return nodeToPolyNumber( node );}
            case STRING -> {return nodeToPolyString( node );}
            case BOOLEAN -> {return nodeToPolyBoolean( node );}
        }
        return new PolyNull();
    }


    public PolyNull nodeToPolyNull() {
        return new PolyNull();
    }


    public PolyString nodeToPolyString( JsonNode node ) {
        return new PolyString( node.asText() );
    }


    public PolyBoolean nodeToPolyBoolean( JsonNode node ) {
        return new PolyBoolean( node.asBoolean() );
    }


    public PolyNumber nodeToPolyNumber( JsonNode node ) {
        if ( node.isIntegralNumber() ) {
            return new PolyLong( node.asLong() );
        }
        return new PolyDouble( node.asDouble() );
    }


    public PolyValue nodeToPolyList( JsonNode node ) {
        return StreamSupport.stream( node.spliterator(), false )
                .map( this::nodeToPolyValue )
                .collect( Collectors.toCollection( PolyList::new ) );
    }


}
