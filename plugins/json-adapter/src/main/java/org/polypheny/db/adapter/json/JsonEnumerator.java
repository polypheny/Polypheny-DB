package org.polypheny.db.adapter.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;

final class JsonEnumerator implements Enumerator<PolyValue[]> {

    private final static ObjectMapper MAPPER = new ObjectMapper();
    private final static JsonToPolyConverter CONVERTER = new JsonToPolyConverter();

    private final URL url;
    private JsonParser parser;
    private PolyValue[] current;
    private boolean isCollection;


    JsonEnumerator( URL url ) {
        this.url = url;
    }


    private void initializeParser() throws IOException {
        if ( this.parser == null ) {
            this.parser = new JsonFactory().createParser( url.openStream() );
            JsonToken token = parser.nextToken();
            isCollection = (token == JsonToken.START_ARRAY);
            if ( !isCollection && token != JsonToken.START_OBJECT ) {
                throw new IllegalArgumentException( "Invalid JSON file format. Expected an array or an object at the top level." );
            }
        }
    }


    private JsonNode getNextNode() throws IOException {
        if ( parser == null || parser.isClosed() ) {
            return null;
        }

        if ( isCollection ) {
            while ( parser.nextToken() != JsonToken.END_ARRAY ) {
                if ( parser.currentToken() != JsonToken.START_OBJECT ) {
                    continue;
                }
                return MAPPER.readTree( parser );
            }
        }

        JsonNode node = null;
        if ( parser.currentToken() == JsonToken.START_OBJECT ) {
            node = MAPPER.readTree( parser );
            isCollection = false;
        }
        return node;
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            if ( parser == null ) {
                initializeParser();
            }
            JsonNode node = getNextNode();
            current = node == null ? null : new PolyValue[]{ CONVERTER.nodeToPolyDocument( node ) };
            return node != null;
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Error reading JSON: " + e.getMessage(), e );
        }
    }


    @Override
    public void reset() {
        close();
        this.parser = null;
        current = null;
    }


    @Override
    public void close() {
        try {
            if ( parser != null ) {
                parser.close();
            }
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Failed to close JSON parser: " + e.getMessage(), e );
        }
    }

}
