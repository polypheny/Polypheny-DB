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
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private int order;
    private String title;
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private String description;
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private String icon;
    @SuppressWarnings({ "unused" })
    private ConcurrentMap<String, Config> configs = new ConcurrentHashMap<>();


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
