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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
     * @param title       Title of this page
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
     * @param id          Id of this page
     * @param title       Title of this page
     * @param description Description of this page, will be displayed in the UI
     */
    public InformationPage( final String id, final String title, final String description ) {
        this.id = id;
        this.name = title;
        this.description = description;
    }


    /**
     * Add a group that belongs to this page
     */
    public void addGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            this.groups.put( g.getId(), g );
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
     * Page is converted to a JSON using gson
     *
     * @return this page as JSON
     */
    public String asJson() {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter( InformationDuration.class, InformationDuration.getSerializer() )
                .registerTypeAdapter( Duration.class, Duration.getSerializer() )
                .disableHtmlEscaping()
                .create();
        return gson.toJson( this );
    }
}
