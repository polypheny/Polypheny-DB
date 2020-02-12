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

package ch.unibas.dmi.dbis.polyphenydb.information;


import ch.unibas.dmi.dbis.polyphenydb.information.exception.InformationRuntimeException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * An InformationPage contains multiple InformationGroups that will be rendered together in a subpage in the UI.
 */
public class InformationPage {


    /**
     * Id of this page
     */
    private final String id;

    /**
     * Name of this page
     */
    private String name; // title

    /**
     * Description for this page
     */
    private String description;

    /**
     * You can set an icon that will be displayed before the label of this page (in the sidebar).
     */
    private String icon;

    /**
     * Is true, if the page was created implicit. If it will be created explicit, additional information (title/description/icon) will be added.
     */
    private boolean implicit = false;

    /**
     * Groups that belong to this page.
     */
    private final ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<>();



    /**
     * Constructor
     *
     * @param id Id of this page
     * @param title Title of this page
     */
    public InformationPage( final String id, final String title ) {
        this.id = id;
        this.name = title;
    }


    /**
     * Constructor
     *
     * @param id Id of this page
     * @param title Title of this page
     * @param description Description of this page, will be displayed in the UI
     */
    public InformationPage( final String id, final String title, final String description ) {
        this( id, title );
        this.description = description;
    }


    /**
     * Constructor
     * When creating a page implicitly
     */
    public InformationPage( final String id ) {
        this.id = id;
        this.name = id;
        this.implicit = true;
        //todo default icon
        this.addGroup( new InformationGroup( id, id ).setImplicit( true ) );
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
     * Get the name of this icon
     */
    public String getIcon() {
        return icon;
    }


    /**
     * Set the icon for this page
     */
    public InformationPage setIcon( final String icon ) {
        this.icon = icon;
        return this;
    }


    /**
     * Get the id of this page
     */
    public String getId() {
        return id;
    }


    /**
     * Get the name of this page
     */
    public String getName() {
        return name;
    }


    /**
     * Get the description of this page
     */
    public String getDescription() {
        return description;
    }


    /**
     * Check if the page was created implicit
     */
    public boolean isImplicit() {
        return this.implicit;
    }


    /**
     * Override an implicit page with an explicit one
     */
    public void overrideWith( final InformationPage page ) {
        if( ! this.implicit ){
            throw new InformationRuntimeException( "Explicitly created pages are not allowed to be overwritten." );
        }else if( page.isImplicit() ){
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
                .setPrettyPrinting()
                .disableHtmlEscaping()
                .create();
        return gson.toJson( this );
    }
}
