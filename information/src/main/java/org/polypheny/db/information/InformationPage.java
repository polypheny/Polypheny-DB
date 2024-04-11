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

package org.polypheny.db.information;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.exception.InformationRuntimeException;


/**
 * An InformationPage contains multiple InformationGroups that will be rendered together in a subpage in the UI.
 */
@Slf4j
@Accessors(chain = true)
public class InformationPage extends Refreshable {


    /**
     * Id of this page
     */
    @JsonProperty
    @Getter
    private final String id;

    /**
     * Name of this page
     */
    @JsonProperty
    @Getter
    private String name; // title

    /**
     * Description for this page
     */
    @JsonProperty
    @Getter
    private String description;

    /**
     * You can set an icon that will be displayed before the label of this page (in the sidebar).
     */
    @JsonProperty
    @Getter
    @Setter
    private String icon;

    /**
     * Is true, if the page was created implicit. If it will be created explicit, additional information (title/description/icon) will be added.
     */
    @Getter
    @JsonProperty
    private boolean implicit = false;

    /**
     * Pages with the same label will be grouped together
     */
    @Getter
    @Setter
    @JsonProperty
    private String label;

    /**
     * All items on this page will be rendered in full width
     */
    @JsonProperty
    private boolean fullWidth = false;

    /**
     * Groups that belong to this page.
     */
    @JsonProperty
    private final Map<String, InformationGroup> groups = new ConcurrentHashMap<>();


    /**
     * Constructor
     *
     * @param title Title of this page
     */
    public InformationPage( final String title ) {
        this( UUID.randomUUID().toString(), title, null );
    }


    /**
     * Constructor
     *
     * @param title Title of this page
     * @param description Description of this page, will be displayed in the UI
     */
    public InformationPage( final String title, final String description ) {
        this( UUID.randomUUID().toString(), title, description );
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
        try {
            return Information.mapper.writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serialization of informationPage" );
            return null;
        }
    }

}
