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

package org.polypheny.db.information;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.information.InformationDuration.Duration;
import org.polypheny.db.information.exception.InformationRuntimeException;


/**
 * An InformationPage contains multiple InformationGroups that will be rendered together in a subpage in the UI.
 */
@Accessors(chain = true)
public class InformationPage extends Refreshable {

    // GsonBuilder, which is able to serialize the underlying InformationGroups
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter( InformationGroup.class, InformationGroup.getSerializer() )
            .registerTypeAdapter( InformationDuration.class, InformationDuration.getSerializer() )
            .registerTypeAdapter( Duration.class, Duration.getSerializer() )
            .registerTypeAdapter( Enum.class, getSerializer() )
            .create();

    /**
     * Id of this page
     */
    @Getter
    private final String id;

    /**
     * Name of this page
     */
    @Getter
    private String name; // title

    /**
     * Description for this page
     */
    @Getter
    private String description;

    /**
     * You can set an icon that will be displayed before the label of this page (in the sidebar).
     */
    @Getter
    @Setter
    private String icon;

    /**
     * Is true, if the page was created implicit. If it will be created explicit, additional information (title/description/icon) will be added.
     */
    @Getter
    private boolean implicit = false;

    /**
     * Pages with the same label will be grouped together
     */
    @Getter
    @Setter
    private String label;

    /**
     * All items on this page will be rendered in full width
     */
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private boolean fullWidth = false;

    /**
     * Groups that belong to this page.
     */
    private final ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<>();


    /**
     * Constructor
     *
     * @param title Title of this page
     */
    public InformationPage( final String title ) {
        this.id = UUID.randomUUID().toString();
        this.name = title;
    }


    /**
     * Constructor
     *
     * @param title Title of this page
     * @param description Description of this page, will be displayed in the UI
     */
    public InformationPage( final String title, final String description ) {
        this.id = UUID.randomUUID().toString();
        this.name = title;
        this.description = description;
    }


    /**
     * Constructor
     *
     * @param id Id of this page
     * @param title Title of this page
     * @param description Description of this page, will be displayed in the UI
     */
    public InformationPage( final String id, final String title, final String description ) {
        this.id = id;
        this.name = title;
        this.description = description;
    }


    private InformationPage( JsonReader in ) throws IOException {
        String id = null;
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "id":
                    id = in.nextString();
                    break;
                case "name":
                    name = in.nextString();
                    break;
                case "description":
                    description = in.nextString();
                    break;
                case "icon":
                    icon = in.nextString();
                    break;
                case "implicit":
                    implicit = in.nextBoolean();
                    break;
                case "label":
                    label = in.nextString();
                    break;
                case "fullWidth":
                    fullWidth = in.nextBoolean();
                    break;
                case "groups":
                    addGroup( gson.fromJson( in, ConcurrentHashMap.class ) );
                    break;
                default:
                    throw new RuntimeException( "Error while deserializing InformationPage." );
            }
        }
        this.id = id;
    }


    /**
     * Add a group that belongs to this page
     */
    public void addGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            this.groups.put( g.getId(), g );
        }
    }


    public void removeGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            this.groups.remove( g.getId(), g );
        }
    }


    /**
     * Display all InformationObjects withing this page with the full width in the UI
     */
    public InformationPage fullWidth() {
        this.fullWidth = true;
        return this;
    }


    /**
     * Override an implicit page with an explicit one
     */
    public void overrideWith( final InformationPage page ) {
        if ( !this.implicit ) {
            throw new InformationRuntimeException( "Explicitly created pages are not allowed to be overwritten." );
        } else if ( page.isImplicit() ) {
            throw new InformationRuntimeException( "A page cannot be overwritten by an implicitly created page." );
        }
        this.name = page.getName();
        this.description = page.getDescription();
        this.icon = page.getIcon();
        this.groups.putAll( page.groups );
        this.implicit = false;
    }


    /**
     * Convert page to Json using the custom TypeAdapter
     *
     * @return this page as JSON
     */
    public String asJson() {
        return gson.toJson( this );
    }


    public static TypeAdapter<InformationPage> getSerializer() {
        return new TypeAdapter<InformationPage>() {
            @Override
            public void write( JsonWriter out, InformationPage value ) throws IOException {
                if ( value == null ) {
                    out.nullValue();
                    return;
                }
                out.beginObject();
                out.name( "id" );
                out.value( value.id );
                out.name( "name" );
                out.value( value.name );
                out.name( "description" );
                out.value( value.description );
                out.name( "icon" );
                out.value( value.icon );
                out.name( "implicit" );
                out.value( value.implicit );
                out.name( "label" );
                out.value( value.label );
                out.name( "fullWidth" );
                out.value( value.fullWidth );
                out.name( "groups" );
                handleGroups( out, value.groups );
                out.endObject();
            }


            private void handleGroups( JsonWriter out, ConcurrentMap<String, InformationGroup> groups ) throws IOException {
                out.beginObject();
                for ( Entry<String, InformationGroup> entry : groups.entrySet() ) {
                    out.name( entry.getKey() );
                    gson.toJson( entry.getValue(), InformationGroup.class, out );
                }
                out.endObject();
            }


            @Override
            public InformationPage read( JsonReader in ) throws IOException {
                if ( in.peek() == JsonToken.NULL ) {
                    in.nextNull();
                    return null;
                }
                in.beginObject();
                InformationPage page = new InformationPage( in );
                in.endObject();
                return page;
            }
        };
    }

}
