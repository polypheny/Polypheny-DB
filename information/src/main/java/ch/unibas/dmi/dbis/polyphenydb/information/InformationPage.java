/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
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
    public InformationPage ( final String id ) {
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
    public void overrideWith ( final InformationPage page ){
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
