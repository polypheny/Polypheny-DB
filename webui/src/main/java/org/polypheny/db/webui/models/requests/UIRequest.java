/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.webui.models.requests;


import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.polypheny.db.webui.models.SortState;


/**
 * Required to parse a request coming from the UI using Gson
 */
public class UIRequest {

    static {

    }


    /**
     * Type of a request, e.g. QueryRequest or RelAlgRequest
     */
    public String requestType;

    /**
     * The name of the table the data should be fetched from
     */
    public String tableId;

    /**
     * Information about the pagination,
     * what current page should be loaded
     */
    public int currentPage;

    /**
     * Data that should be inserted
     */
    public Map<String, String> data;

    /**
     * For each column: If it should be filtered empty string if it should not be filtered
     */
    public Map<String, String> filter;

    /**
     * For each column: If and how it should be sorted
     */
    public Map<String, SortState> sortState;

    /**
     * Request to fetch a result without a limit. Default false.
     */
    public boolean noLimit;


    public UIRequest() {
        // empty on purpose
    }


    private UIRequest( JsonReader in ) throws IOException {
        while ( in.peek() != JsonToken.NULL ) {
            switch ( in.nextName() ) {
                case "requestType":
                    requestType = in.nextString();
                    break;
                case "tableId":
                    tableId = in.nextString();
                    break;
                case "currentPage":
                    currentPage = in.nextInt();
                    break;
                case "data":
                    data = stringMapAdapter.read( in );
                    break;
                case "filter":
                    filter = stringMapAdapter.read( in );
                    break;
                case "sortState":
                    sortState = sortStateMapAdapter.read( in );
                    break;
                case "noLimit":
                    noLimit = in.nextBoolean();
                    break;
                default:
                    throw new RuntimeException( "Error while deserializing UIRequest." );
            }
        }
    }


    public String getSchemaName() {
        if ( tableId != null ) {
            return tableId.split( "\\." )[0];
        }
        return null;
    }


    public String getTableName() {
        if ( tableId != null ) {
            return tableId.split( "\\." )[1];
        }
        return null;
    }


    static BiConsumer<JsonWriter, String> stringSerializer = ( out, val ) -> {
        try {
            out.value( val );
        } catch ( IOException e ) {
            throw new RuntimeException( "Error while serializing string." );
        }
    };

    static BiConsumer<JsonWriter, SortState> sortSerializer = ( out, val ) -> {
        try {
            SortState.getSerializer().write( out, val );
        } catch ( IOException e ) {
            throw new RuntimeException( "Error while serializing sort." );
        }
    };

    static final TypeAdapter<Map<String, String>> stringMapAdapter = getMapTypeAdapter( stringSerializer, ( e ) -> {
        try {
            return e.nextString();
        } catch ( IOException ex ) {
            throw new RuntimeException( "Error while deserializing string." );
        }
    } );
    static final TypeAdapter<Map<String, SortState>> sortStateMapAdapter = getMapTypeAdapter( sortSerializer, ( e ) -> {
        try {
            return SortState.getSerializer().read( e );
        } catch ( IOException ex ) {
            throw new RuntimeException( "Error while deserializing string." );
        }
    } );


    public static TypeAdapter<UIRequest> getSerializer() {
        return new TypeAdapter<UIRequest>() {
            @Override
            public void write( JsonWriter out, UIRequest value ) throws IOException {
                if ( value == null ) {
                    out.nullValue();
                    return;
                }
                out.beginObject();
                out.name( "requestType" );
                out.value( value.requestType );
                out.name( "tableId" );
                out.value( value.tableId );
                out.name( "currentPage" );
                out.value( value.currentPage );
                out.name( "data" );
                stringMapAdapter.write( out, value.data );
                out.name( "filter" );
                stringMapAdapter.write( out, value.filter );
                out.name( "sortState" );
                sortStateMapAdapter.write( out, value.sortState );
                out.name( "noLimit" );
                out.value( value.noLimit );
                out.endObject();
            }


            @Override
            public UIRequest read( JsonReader in ) throws IOException {
                if ( in.peek() == JsonToken.NULL ) {
                    in.nextNull();
                    return null;
                }
                in.beginObject();
                UIRequest request = new UIRequest( in );
                in.endObject();
                return request;
            }
        };
    }


    private static <E> TypeAdapter<Map<String, E>> getMapTypeAdapter( BiConsumer<JsonWriter, E> valSerializer, Function<JsonReader, E> valDeserializer ) {
        return new TypeAdapter<Map<String, E>>() {
            @Override
            public void write( JsonWriter out, Map<String, E> value ) throws IOException {
                if ( value == null ) {
                    out.nullValue();
                    return;
                }
                out.beginObject();
                for ( Entry<String, E> entry : value.entrySet() ) {
                    out.beginObject();
                    out.name( entry.getKey() );
                    valSerializer.accept( out, entry.getValue() );
                    out.endObject();
                }
                out.endObject();
            }


            @Override
            public Map<String, E> read( JsonReader in ) throws IOException {
                if ( in.peek() == JsonToken.NULL ) {
                    in.nextNull();
                    return null;
                }
                Map<String, E> map = new HashMap<>();
                in.beginObject();
                while ( in.peek() != JsonToken.END_OBJECT ) {
                    in.beginObject();
                    map.put( in.nextName(), valDeserializer.apply( in ) );
                    in.endObject();
                }
                in.endObject();
                return map;
            }
        };
    }

}
