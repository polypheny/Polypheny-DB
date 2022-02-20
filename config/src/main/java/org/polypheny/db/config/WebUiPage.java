/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;


/**
 * Page for the WebUi containing WebUiGroups that contain configuration elements.
 */
@Accessors(chain = true)
public class WebUiPage {

    @Getter
    private String id;
    @Getter
    private String title;
    @Setter
    @Getter
    private String label;
    private String description;
    @Getter
    private String icon;
    private WebUiPage parentPage;
    private ConcurrentMap<String, WebUiGroup> groups = new ConcurrentHashMap<>();


    /**
     * Constructor
     *
     * @param id Unique ID for the page
     */
    public WebUiPage( final String id ) {
        this.id = id;
    }


    public WebUiPage( final String id, final String title, final String description ) {
        this.id = id;
        this.title = title;
        this.description = description;
    }


    public WebUiPage withIcon( final String icon ) {
        this.icon = icon;
        return this;
    }


    public WebUiPage withParent( final WebUiPage parent ) {
        this.parentPage = parent;
        return this;
    }


    public WebUiPage getParent() {
        return this.parentPage;
    }


    public boolean hasTitle() {
        return this.title != null;
    }


    /**
     * Add a WebUiGroup to this WebUiPage
     *
     * @param g The group to add to this page.
     */
    public void addWebUiGroup( final WebUiGroup g ) {
        groups.put( g.getId(), g );
    }


    /**
     * Serialize this as JSON.
     *
     * @return WebUiPage as JSON
     */
    @Override
    public String toString() {

        TypeAdapter<Enum<?>> enumTypeAdapter = new TypeAdapter<>() {
            @Override
            public void write( JsonWriter out, Enum value ) throws IOException {
                out.beginObject();
                out.name( "clazz" );
                if ( value == null ) {
                    out.value( "null" );
                } else {
                    out.value( value.getClass().toString() );
                    out.name( "all" );
                    out.value( new ObjectMapper().writeValueAsString( value ) );
                }
                out.endObject();
            }


            @Override
            public Enum<?> read( JsonReader in ) throws IOException {
                try {
                    in.nextName();
                    String clazz = in.nextString();
                    if ( clazz.equals( "null" ) ) {
                        return null;
                    } else {
                        in.nextName();
                        String e = in.nextString();
                        return (Enum<?>) new ObjectMapper().readValue( e, Class.forName( clazz ) );
                    }
                } catch ( ClassNotFoundException e ) {
                    throw new RuntimeException( "The Enum was not serializable." );
                }
            }
        };

        Gson gson = new GsonBuilder()
                .registerTypeAdapter( Enum.class, enumTypeAdapter )
                .enableComplexMapKeySerialization()
                .setPrettyPrinting()
                .create();
        return gson.toJson( this );
    }

}
