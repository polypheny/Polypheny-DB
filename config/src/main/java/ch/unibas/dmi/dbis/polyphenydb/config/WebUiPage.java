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

package ch.unibas.dmi.dbis.polyphenydb.config;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Page for the WebUi containing WebUiGroups that contain configuration elements.
 */
public class WebUiPage {

    private String id;
    private String title;
    private String description;
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


    public String getId() {
        return id;
    }


    public String getTitle() {
        return title;
    }


    public String getIcon() {
        return icon;
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

        // https://stackoverflow.com/questions/15736654/how-to-handle-deserializing-with-polymorphism
        /*RuntimeTypeAdapterFactory<Config> runtimeTypeAdapterFactory = RuntimeTypeAdapterFactory.of(Config.class, "configType")
                .registerSubtype( ConfigInteger.class, "Integer" )
                .registerSubtype( ConfigNumber.class, "Number" )
                .registerSubtype( ConfigString.class, "String" );*/

        //Gson gson = new GsonBuilder().registerTypeAdapterFactory( runtimeTypeAdapterFactory ).setPrettyPrinting().create();
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();
        //Gson gson = new Gson();
        return gson.toJson( this );
    }
}
