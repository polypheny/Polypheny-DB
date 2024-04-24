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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

public class JsonEnumerator implements Enumerator<PolyValue> {

    private ObjectMapper mapper;
    private JsonParser parser;
    private boolean isCollection;
    private boolean hasNext;


    public JsonEnumerator( URI uri ) throws IOException {
        this.mapper = new ObjectMapper();
        this.parser = new JsonFactory().createParser( new File( uri ) );
        JsonToken token = parser.nextToken();
        isCollection = token == JsonToken.START_ARRAY;
        hasNext = token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT;
    }


    private JsonNode getNextNode() throws IOException {
        if ( !hasNext ) {
            return null;
        }
        return isCollection ? getNextNodeInCollection() : getSingleNode();
    }


    private JsonNode getNextNodeInCollection() throws IOException {
        JsonNode node = null;
        while ( parser.nextToken() != JsonToken.END_ARRAY ) {
            if ( parser.currentToken() == JsonToken.START_OBJECT ) {
                node = mapper.readTree( parser );
                break;
            }
        }
        return node;
    }


    private JsonNode getSingleNode() throws IOException {
        JsonNode node = null;
        if ( parser.currentToken() == JsonToken.START_OBJECT ) {
            node = mapper.readTree( parser );
        }
        hasNext = false;
        return node;
    }


    private PolyDocument createDocumentFronNode( JsonNode node ) {
        return null;
    }


    @Override
    public PolyValue current() {
        return null;
    }


    @Override
    public boolean moveNext() {
        return false;
    }


    @Override
    public void reset() {

    }


    @Override
    public void close() {
        try {
            parser.close();
        } catch ( IOException e ) {
            throw new RuntimeException( "Failed to close json parser." );
        }
    }

}
