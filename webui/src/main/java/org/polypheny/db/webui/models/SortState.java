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

package org.polypheny.db.webui.models;


import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;

/**
 * Defines how a column is sorted.
 * Required for Gson.
 */
public class SortState {

    /**
     * How the column is supposed to be sorted (ASC or DESC)
     */
    public SortDirection direction;


    /**
     * If true, the column will be sorted
     */
    public boolean sorting;


    /**
     * Column to be sorted
     * needed for the PlanBuilder
     */
    public String column;


    public SortState() {
        this.direction = SortDirection.DESC;
        this.sorting = false;
    }


    public SortState( final SortDirection direction ) {
        this.direction = direction;
        this.sorting = true;
    }


    private SortState( JsonReader in ) throws IOException {
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "direction":
                    direction = SortDirection.valueOf( in.nextString() );
                    break;
                case "sorting":
                    sorting = in.nextBoolean();
                    break;
                case "column":
                    column = in.nextString();
                    break;
                default:
                    throw new RuntimeException( "There was an unrecognized column while deserializing SortState." );
            }

        }
    }


    public static TypeAdapter<SortState> getSerializer() {
        return new TypeAdapter<SortState>() {
            @Override
            public void write( JsonWriter out, SortState state ) throws IOException {
                if ( state == null ) {
                    out.nullValue();
                    return;
                }

                out.beginObject();
                out.name( "direction" );
                out.value( state.direction.name() );
                out.name( "sorting" );
                out.value( state.sorting );
                out.name( "column" );
                out.value( state.column );
                out.endObject();
            }


            @Override
            public SortState read( JsonReader in ) throws IOException {
                if ( in.peek() == null ) {
                    in.nextNull();
                    return null;
                }
                in.beginObject();
                SortState state = new SortState( in );
                in.endObject();
                return state;
            }
        };
    }

}
