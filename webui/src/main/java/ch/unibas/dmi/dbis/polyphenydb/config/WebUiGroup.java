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
 * A group in the WebUi containing multiple configuration fields. Is part of a WebUiPage.
 */
public class WebUiGroup {

    private Integer id;
    private Integer pageId;
    //int order;//schon mit id
    private String title;
    private String description;
    private String icon;
    private ConcurrentMap<String, Config> configs = new ConcurrentHashMap<String, Config>();


    /**
     * @param id unique id of this group
     * @param pageId id of WebUiPage this group belongs to
     */
    public WebUiGroup( int id, int pageId ) {
        this.id = id;
        this.pageId = pageId;
    }


    /**
     * set the title of this group
     */
    public WebUiGroup withTitle( String title ) {
        this.title = title;
        return this;
    }


    /**
     * set the description of this group
     */
    public WebUiGroup withDescription( String description ) {
        this.description = description;
        return this;
    }


    public WebUiGroup withIcon( String icon ) {
        this.icon = icon;
        return this;
    }


    public boolean hasTitle() {
        return this.title != null;
    }


    public int getId() {
        return id;
    }


    public int getPageId() {
        return pageId;
    }


    /**
     * applies all attributes of group g to this existing WebUiGroup object
     *
     * @param g group with more attributes
     */
    public WebUiGroup override( WebUiGroup g ) {
        if ( g.id != null ) {
            this.id = g.id;
        }
        if ( g.pageId != null ) {
            this.pageId = g.pageId;
        }
        if ( g.title != null ) {
            this.title = g.title;
        }
        if ( g.description != null ) {
            this.description = g.description;
        }
        if ( g.icon != null ) {
            this.icon = g.icon;
        }
        return this;
    }


    /**
     * add a configuration object that should be displayed in this group
     */
    public void addConfig( Config c ) {
        if ( this.configs.get( c.getKey() ) != null ) {
            this.configs.get( c.getKey() ).override( c );
        } else {
            this.configs.put( c.getKey(), c );
        }
    }


    /**
     * @return returns WebUiPage as json
     */
    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }
}
