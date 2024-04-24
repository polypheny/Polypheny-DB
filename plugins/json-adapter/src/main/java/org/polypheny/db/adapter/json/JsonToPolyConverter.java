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
import org.polypheny.db.type.entity.numerical.PolyInteger;

public class JsonToPolyConverter {

    public PolyDocument nodeToDocument( JsonNode node ) throws IOException {
        return new PolyDocument( nodeToPolyMap( node ) );
    }


    public HashMap<PolyString, PolyValue> nodeToPolyMap( JsonNode node ) {
        HashMap<PolyString, PolyValue> polyMap = new HashMap<>();
        node.fields().forEachRemaining( entry -> {
            PolyString key = new PolyString( entry.getKey() );
            PolyValue value = nodeToPolyValue( entry.getValue() );
            polyMap.put( key, value );
        } );
        return polyMap;
    }


    public PolyValue nodeToPolyValue( JsonNode node ) {
        switch ( node.getNodeType() ) {
            case NULL -> {
            }
            case ARRAY -> {
            }
            case OBJECT -> {
            }
            case NUMBER -> {
            }
            case STRING -> {
            }
            case BOOLEAN -> {
            }
        }
        return new PolyNull();
    }

    public PolyNull nodeToPolyNull() {
        return new PolyNull();
    }

    public PolyString nodeToPolyString(JsonNode node) {
        return new PolyString( node.textValue() );
    }

    public PolyBoolean nodeToPolyBoolean(JsonNode node) {
        return new PolyBoolean( node.booleanValue() );
    }

    public PolyNumber nodeToPolyNumber(JsonNode node) {
        Number number = node.numberValue();
        return new PolyInteger( 69 );
    }


    public PolyValue arrayNodeToPolyList( JsonNode node ) {
        return StreamSupport.stream( node.spliterator(), false )
                .map( this::nodeToPolyValue )
                .collect( Collectors.toCollection( PolyList::new ) );
    }


}
