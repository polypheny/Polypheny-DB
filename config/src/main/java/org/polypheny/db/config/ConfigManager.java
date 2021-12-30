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

package org.polypheny.db.config;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.exception.ConfigRuntimeException;


/**
 * ConfigManager allows to add and retrieve configuration objects.
 * If the configuration element has a Web UI Group and Web UI Page defined, it can be requested from the Web UI and the value of the configuration can be changed there.
 */
public class ConfigManager {

    private static ConfigManager instance = new ConfigManager();

    private final ConcurrentMap<String, Config> configs;
    private final ConcurrentMap<String, WebUiGroup> uiGroups;
    private final ConcurrentMap<String, WebUiPage> uiPages;

    private com.typesafe.config.Config configFile;


    private ConfigManager() {
        this.configs = new ConcurrentHashMap<>();
        this.uiGroups = new ConcurrentHashMap<>();
        this.uiPages = new ConcurrentHashMap<>();

        configFile = ConfigFactory.load();
    }


    /**
     * Singleton
     */
    public static ConfigManager getInstance() {
        return instance;
    }


    /**
     * Register a configuration element in the ConfigManager.
     *
     * @param config Configuration element to register.
     * @throws ConfigRuntimeException If a Config is already registered.
     */
    public void registerConfig( final Config config ) {
        if ( this.configs.containsKey( config.getKey() ) ) {
            throw new ConfigRuntimeException( "Cannot register two configuration elements with the same key: " + config.getKey() );
        } else {
            // Check if the config file contains this key and if so set the value to the one defined in the config file
            if ( configFile.hasPath( config.getKey() ) ) {
                config.setValueFromFile( configFile );
            }
            this.configs.put( config.getKey(), config );
        }
    }


    /**
     * Register multiple configuration elements in the ConfigManager.
     *
     * @param configs Configuration elements to register
     */
    public void registerConfigs( final Config... configs ) {
        for ( Config c : configs ) {
            this.registerConfig( c );
        }
    }


    public void observeAll( final ConfigListener listener ) {
        for ( Config c : configs.values() ) {
            c.addObserver( listener );
        }
    }


    /**
     * Get configuration as Configuration object
     */
    public Config getConfig( final String s ) {
        return configs.get( s );
    }


    /**
     * Register a Web UI Group in the ConfigManager.
     * A Web UI Group consists of several Configs that will be displayed together in the Web UI.
     *
     * @param group WebUiGroup to register
     * @throws ConfigRuntimeException If a group with that key already exists.
     */
    public void registerWebUiGroup( final WebUiGroup group ) {
        if ( this.uiGroups.containsKey( group.getId() ) ) {
            throw new ConfigRuntimeException( "Cannot register two WeUiGroups with the same key: " + group.getId() );
        } else {
            this.uiGroups.put( group.getId(), group );
        }
    }


    /**
     * Register a Web UI Page in the ConfigManager.
     * A Web UI Page consists of several Web UI Groups that will be displayed together in the Web UI.
     *
     * @param page WebUiPage to register
     * @throws ConfigRuntimeException If a page with that key already exists.
     */
    public void registerWebUiPage( final WebUiPage page ) {
        if ( this.uiPages.containsKey( page.getId() ) ) {
            throw new ConfigRuntimeException( "Cannot register two WebUiPages with the same key: " + page.getId() );
        } else {
            this.uiPages.put( page.getId(), page );
        }
    }


    /**
     * Generates a Json of all the Web UI Pages in the ConfigManager (for the sidebar in the Web UI)
     * The Json does not contain the groups and configs of the Web UI Pages
     */
    public String getWebUiPageList() {
        //todo recursion with parentPage field
        // Angular wants: { id, name, icon, children[] }
        ArrayList<PageListItem> out = new ArrayList<>();
        for ( WebUiPage p : uiPages.values() ) {
            out.add( new PageListItem( p.getId(), p.getTitle(), p.getIcon(), p.getLabel() ) );
        }
        out.sort( Comparator.comparing( PageListItem::getName ) );
        Gson gson = new GsonBuilder()
                .enableComplexMapKeySerialization()
                .serializeNulls()
                .create();
        return gson.toJson( out );
    }


    /**
     * Get certain page as json.
     * Groups within a page and configs within a group are sorted in the Web UI, not here.
     *
     * @param id The id of the page
     */
    public String getPage( final String id ) {
        // fill WebUiGroups with Configs
        for ( ConcurrentMap.Entry<String, Config> c : configs.entrySet() ) {
            try {
                String i = c.getValue().getWebUiGroup();
                this.uiGroups.get( i ).addConfig( c.getValue() );
            } catch ( NullPointerException e ) {
                // TODO: This is not nice...
                // Skipping config with no WebUiGroup
            }
        }

        // fill WebUiPages with WebUiGroups
        for ( ConcurrentMap.Entry<String, WebUiGroup> g : uiGroups.entrySet() ) {
            try {
                String i = g.getValue().getPageId();
                this.uiPages.get( i ).addWebUiGroup( g.getValue() );
            } catch ( NullPointerException e ) {
                // TODO: This is not nice...
                // Skipping config with no page id
            }
        }
        return uiPages.get( id ).toString();
    }


    /**
     * The class PageListItem will be converted into a Json String by Gson.
     * The Web UI requires a Json Object with the fields id, name, icon, children[] for the Sidebar.
     * This class is required to convert a WebUiPage object into the format needed by the Angular WebUi
     */
    static class PageListItem {

        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        private String id;
        @Getter
        private String name;
        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        private String icon;
        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        private String label;
        @SuppressWarnings({ "unused" })
        private PageListItem[] children;


        PageListItem( final String id, final String name, final String icon, final String label ) {
            this.id = id;
            this.name = name;
            this.icon = icon;
            this.label = label;
        }


        @Override
        public String toString() {
            Gson gson = new Gson();
            return gson.toJson( this );
        }

    }

}
