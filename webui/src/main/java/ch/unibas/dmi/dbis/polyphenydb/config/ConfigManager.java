/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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
 */

package ch.unibas.dmi.dbis.polyphenydb.config;

import ch.unibas.dmi.dbis.polyphenydb.config.Config.ConfigListener;
import ch.unibas.dmi.dbis.polyphenydb.config.exception.ConfigException;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//todo observable
/** ConfigManager where you can add a configuration (Config object) that is needed in different Classes.
 * If the configuration object has a WebUiGroup and WebUiPage, it can be requested from the WebUi and the value of the configuration can be changed there. */
public class ConfigManager {

    private static ConfigManager instance;

    private ConcurrentMap<String, Config> configs;
    private ConcurrentMap<String, WebUiGroup> uiGroups;
    private ConcurrentMap<String, WebUiPage> uiPages;

    private ConfigManager() {
        this.configs = new ConcurrentHashMap<String, Config>();
        this.uiGroups = new ConcurrentHashMap<String, WebUiGroup>();
        this.uiPages = new ConcurrentHashMap<String, WebUiPage>();
    }

    /** singleton */
    public static ConfigManager getInstance () {
        if(instance == null) {
            instance = new ConfigManager();
        }
        return instance;
    }

    /** add a configuration to the ConfigManager
     * @param config configuration object of type Config */
    public boolean registerConfig( Config config) {
        if ( this.configs.get( config.getKey() ) != null ) {
            throw new ConfigException( "Cannot register Config twice: " + config.getKey() );
            //this.configs.get( config.getKey() ).override( config );
        }else {
            this.configs.put( config.getKey(),config );
        }
        return true;
    }

    public boolean registerConfigs ( Config... configs ) {
        boolean successful = true;
        for ( Config c : configs ) {
            if( !this.registerConfig( c ) ) successful = false;
        }
        return successful;
    }

    public void observeAll ( ConfigListener listener ) {
        for( Config c : configs.values() ) {
            c.addObserver( listener );
        }
    }

    /**
     * @param key (unique) key of the configuration
     * */
    public Object getObject ( String key) {
        return configs.get( key ).getObject();
    }

    /** get configuration as int */
    public int getInt ( String key ) {
        return (int) configs.get( key ).getInt();
    }

    /** get configuration as String*/
    public String getString ( String key ) {
        return (String) configs.get( key ).getString();
    }

    /** get configuration as Configuration object */
    public Config getConfig( String s ) {
        return configs.get( s );
    }

    /** add a WebUiGroup to the ConfigManager
     * @param g WebUiGroup to add */
    public void addUiGroup ( WebUiGroup g ) {
        if ( this.uiGroups.get( g.getId() ) != null ){
            this.uiGroups.get( g.getId() ).override( g );
        } else {
            this.uiGroups.put( g.getId(), g );
        }
    }

    /** add a WebUiPage to the ConfigManager
     * @param p WebUiPage to add */
    public void addUiPage ( WebUiPage p ) {
        if ( this.uiPages.get( p.getId() ) != null ) {
            this.uiPages.get( p.getId() ).override( p );
        } else {
            this.uiPages.put( p.getId(), p );
        }
    }

    /** get simple list of pages, without their groups and configs (for WebUi Sidebar) */
    public String getPageList () {
        //todo recursion with parentPage field
        // Angular wants: { id, name, icon, children[] }
        ArrayList<PageListItem> out = new ArrayList<PageListItem>();
        for ( WebUiPage p: uiPages.values() ){
            out.add( new PageListItem( p.getId(), p.getTitle(), p.getIcon() ) );
        }
        Gson gson = new Gson();
        return gson.toJson( out );
    }

    /** get certain page as json
     * @param id pageId */
    //todo sort
    public String getPage ( String id ) {
        //fill WebUiGroups with Configs
        for( ConcurrentMap.Entry<String, Config> c : configs.entrySet()){
            try{
                String i = c.getValue().getWebUiGroup();
                this.uiGroups.get( i ).addConfig( c.getValue() );
            } catch ( NullPointerException e ){
                //System.out.println("skipping config "+c.getKey()+" with no WebUiGroup");
            }
        }

        //fill WebUiPages with WebUiGroups
        for( ConcurrentMap.Entry<String, WebUiGroup> g : uiGroups.entrySet() ){
            try{
                String i = g.getValue().getPageId();
                this.uiPages.get( i ).addWebUiGroup( g.getValue() );
            } catch ( NullPointerException e ){
                //System.out.println("skipping group "+g.getKey()+" with no pageid");
            }
        }
        return uiPages.get( id ).toString();
    }

}

class PageListItem{
    private String id;
    private String name;
    private String icon;
    private PageListItem[] children;
    public PageListItem ( String id, String name, String icon ) {
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
