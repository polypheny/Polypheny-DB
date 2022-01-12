/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.information;


import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.UUID;


public class InformationStacktrace extends Information {

    Throwable exception;


    /**
     * Constructor
     *
     * @param group The InformationGroup which this information belongs
     * @param exception A throwable to display in the UI
     */
    public InformationStacktrace( final Throwable exception, final InformationGroup group ) {
        super( UUID.randomUUID().toString(), group.getId() );
        this.exception = exception;
    }


    public InformationStacktrace( JsonReader in ) throws IOException {
        super( UUID.randomUUID().toString(), peekGroupId( in ) );
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "id":
                    in.nextString();
                    break;
                case "type":
                    type = in.nextString();
                    break;
                case "uiOrder":
                    setUiOrder( in.nextInt() );
                    break;
                default:
                    throw new RuntimeException( "Error while deserializing InformationStacktrace." );
            }
        }
    }


    private static String peekGroupId( JsonReader in ) throws IOException {
        if ( !in.nextName().equals( "groupId" ) ) {
            throw new RemoteException( "The serialization of InformationStacktrace threw an error." );
        }
        return in.nextString();
    }


    public static TypeAdapter<InformationStacktrace> getSerializer() {
        return new TypeAdapter<InformationStacktrace>() {
            @Override
            public void write( JsonWriter out, InformationStacktrace value ) throws IOException {
                if ( value == null ) {
                    out.nullValue();
                    return;
                }
                out.beginObject();
                out.name( "groupId" );
                out.value( value.getGroup() );
                out.name( "id" );
                out.value( value.getId() );
                out.name( "type" );
                out.value( value.type );
                out.name( "uiOrder" );
                out.value( value.getUiOrder() );
                out.name( "exception" );
                if ( value.exception == null ) {
                    out.nullValue();
                } else {
                    out.beginObject();
                    out.name( "message" );
                    out.value( value.exception.getMessage() );
                    out.endObject();
                }
                out.endObject();
            }


            @Override
            public InformationStacktrace read( JsonReader in ) throws IOException {
                if ( in.peek() == JsonToken.NULL ) {
                    in.nextNull();
                    return null;
                }

                in.beginObject();
                InformationStacktrace info = new InformationStacktrace( in );
                in.endObject();
                return info;
            }
        };
    }

}
