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


import ch.unibas.dmi.dbis.polyphenydb.config.Config.ConfigListener;
import ch.unibas.dmi.dbis.polyphenydb.config.exception.ConfigRuntimeException;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * ConfigManager where you can add a configuration (Config object) that is needed in different Classes.
 * If the configuration element has a WebUiGroup and WebUiPage, it can be requested from the WebUi and the value of the configuration can be changed there.
 */
public class ConfigManager {

    private static ConfigManager instance;

    private ConcurrentMap<String, Config> configs;
    private ConcurrentMap<String, WebUiGroup> uiGroups;
    private ConcurrentMap<String, WebUiPage> uiPages;


    private ConfigManager() {
        this.configs = new ConcurrentHashMap<>();
        this.uiGroups = new ConcurrentHashMap<>();
        this.uiPages = new ConcurrentHashMap<>();
    }


    /**
     * Singleton
     */
    public static ConfigManager getInstance() {
        if ( instance == null ) {
            instance = new ConfigManager();
        }
        return instance;
    }


    /**
     * Register a configuration element in the ConfigManager.
     * throws a ConfigRuntimeException if a Config is already registered.
     * @param config Configuration element to register
     */
    public void registerConfig( final Config config ) {
        if ( this.configs.get( config.getKey() ) != null ) {
            throw new ConfigRuntimeException( "Cannot register two configuration elements with the same key: " + config.getKey() );
        } else {
            this.configs.put( config.getKey(), config );
        }
    }


    /**
     * Register multiple configuration elements in the ConfigManager.
     *
     * @param configs Configuration elements to register
     */
    public void registerConfigs( Config... configs ) {
        for ( Config c : configs ) {
            this.registerConfig( c );
        }
    }


    public void observeAll( ConfigListener listener ) {
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
     * Register a WebUiGroup to the ConfigManager.
     * A WebUiGroup consists of several Configs that will be displayed together in the Angular WebUi.
     *
     * @param g WebUiGroup to register
     */
    public void registerWebUiGroup( final WebUiGroup g ) {
        if ( this.uiGroups.get( g.getId() ) != null ) {
            throw new ConfigRuntimeException( "Cannot register two WeUiGroups with the same key: " + g.getId() );
        } else {
            this.uiGroups.put( g.getId(), g );
        }
    }


    /**
     * Register a WebUiPage to the ConfigManager.
     * A WebUiPage consists of several WebUiGroups that will be displayed together in the Angular WebUi.
     *
     * @param p WebUiPage to register
     */
    public void registerWebUiPage( final WebUiPage p ) {
        if ( this.uiPages.get( p.getId() ) != null ) {
            throw new ConfigRuntimeException( "Cannot register two WebUiPages with the same key: " + p.getId() );
        } else {
            this.uiPages.put( p.getId(), p );
        }
    }


    /**
     * Generates a Json of all the WebUiPages in the ConfigManager (for the Sidebar in the Angular WebUi)
     * The Json does not contain the groups and configs of the WebUiPages
     */
    public String getWebUiPageList() {
        //todo recursion with parentPage field
        // Angular wants: { id, name, icon, children[] }
        ArrayList<PageListItem> out = new ArrayList<PageListItem>();
        for ( WebUiPage p : uiPages.values() ) {
            out.add( new PageListItem( p.getId(), p.getTitle(), p.getIcon() ) );
        }
        Gson gson = new Gson();
        return gson.toJson( out );
    }


    /**
     * Get certain page as json
     * Groups within a page and Configs within a group are sorted in the Angular app, not here.
     *
     * @param id pageId
     */
    public String getPage ( String id ) {
        //fill WebUiGroups with Configs
        for( ConcurrentMap.Entry<String, Config> c : configs.entrySet()){
            try{
                String i = c.getValue().getWebUiGroup();
                this.uiGroups.get( i ).addConfig( c.getValue() );
            } catch ( NullPointerException e ) {
                //System.out.println("skipping config "+c.getKey()+" with no WebUiGroup");
            }
        }

        //fill WebUiPages with WebUiGroups
        for( ConcurrentMap.Entry<String, WebUiGroup> g : uiGroups.entrySet() ){
            try{
                String i = g.getValue().getPageId();
                this.uiPages.get( i ).addWebUiGroup( g.getValue() );
            } catch ( NullPointerException e ) {
                //System.out.println("skipping group "+g.getKey()+" with no pageid");
            }
        }
        return uiPages.get( id ).toString();
    }


    /**
     * The class PageListItem will be converted into a Json String by Gson
     * The Angular WebUi requires a Json Object with the fields id, name, icon, children[] for the Sidebar
     * This class is needed to convert a WebUiPage object into the format needed by the Angular WebUi
     */
    class PageListItem {

        private String id;
        private String name;
        private String icon;
        private PageListItem[] children;


        public PageListItem( String id, String name, String icon ) {
            this.id = id;
            this.name = name;
            this.icon = icon;
        }


        @Override
        public String toString() {
            Gson gson = new Gson();
            return gson.toJson( this );
        }
    }


}
