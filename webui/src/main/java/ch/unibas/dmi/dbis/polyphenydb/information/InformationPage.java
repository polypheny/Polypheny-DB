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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * an InformationPage contains multiple InformationGroups that will be rendered together in a subpage in the UI
 */
public class InformationPage {


    /**
     * id of this page
     */
    private final String id;


    /**
     * name of this page
     */
    private String name; // title


    /**
     * description for this page
     */
    private String description;


    /**
     * you can set an icon that will be displayed before the label of this page (in the sidebar)
     */
    private String icon;
    private final ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<>();


    /**
     * constructor
     *
     * @param id id of this page
     * @param title title of this page
     */
    public InformationPage( final String id, final String title ) {
        this.id = id;
        this.name = title;
    }


    /**
     * constructor
     * @param id id of this page
     * @param title title of this page
     * @param description description of this page, will be displayed in the UI
     */
    public InformationPage( final String id, final String title, final String description ) {
        this( id, title );
        this.description = description;
    }


    /**
     * add a group that belongs to this page
     */
    public void addGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            this.groups.put( g.getId(), g );
        }
    }


    /**
     * get the name of this icon
     */
    public String getIcon() {
        return icon;
    }


    /**
     * set the icon for this page
     */
    public void setIcon( final String icon ) {
        this.icon = icon;
    }


    /**
     * get the id of this page
     */
    public String getId() {
        return id;
    }


    /**
     * get the name of this page
     */
    public String getName() {
        return name;
    }


    /**
     * get the description of this page
     */
    public String getDescription() {
        return description;
    }


    /**
     * page is converted to a JSON using gson
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
