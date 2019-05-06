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

package ch.unibas.dmi.dbis.polyphenydb.config;


import com.google.gson.Gson;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * A group in the Web UI containing multiple configuration fields. Is part of a WebUiPage.
 */
public class WebUiGroup {

    private String id;
    private String pageId;
    /**
     * Field "order" is used by Gson.
     * Groups with lower order are rendered first in the GUI
     */
    private int order;
    private String title;
    private String description;
    private String icon;
    private ConcurrentMap<String, Config> configs = new ConcurrentHashMap<String, Config>();


    /**
     * Constructor
     *
     * @param id unique id of this group
     * @param pageId id of WebUiPage this group belongs to
     */
    public WebUiGroup( final String id, final String pageId ) {
        this.id = id;
        this.pageId = pageId;
    }


    /**
     * Constructor
     *
     * @param id unique id of this group
     * @param pageId id of WebUiPage this group belongs to
     * @param order Group with lower order is rendered first in the GUI. The ordering is happening in the Web UI.
     */
    public WebUiGroup( final String id, final String pageId, final int order ) {
        this.id = id;
        this.pageId = pageId;
        this.order = order;
    }


    /**
     * Set the title of this group
     */
    public WebUiGroup withTitle( final String title ) {
        this.title = title;
        return this;
    }


    /**
     * Set the description of this group
     */
    public WebUiGroup withDescription( final String description ) {
        this.description = description;
        return this;
    }


    public WebUiGroup withIcon( final String icon ) {
        this.icon = icon;
        return this;
    }


    public boolean hasTitle() {
        return this.title != null;
    }


    public String getId() {
        return id;
    }


    public String getPageId() {
        return pageId;
    }


    /**
     * Add a configuration object that should be displayed in this group.
     *
     * @param c The config to add to this group.
     */
    public void addConfig( final Config c ) {
        this.configs.put( c.getKey(), c );

    }


    /**
     * Serialize object as JSON.
     *
     * @return returns WebUiPage as JSON.
     */
    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }
}
