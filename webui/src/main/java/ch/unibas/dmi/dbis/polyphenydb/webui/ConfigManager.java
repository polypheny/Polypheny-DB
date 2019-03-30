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

package ch.unibas.dmi.dbis.polyphenydb.webui;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//todo observable
/** ConfigManager where you can add a configuration (Config object) that is needed in different Classes.
 * If the configuration object has a WebUiGroup and WebUiPage, it can be requested from the WebUi and the value of the configuration can be changed there. */
public class ConfigManager {

    private static ConfigManager instance;

    private ConcurrentMap<String, Config> config;
    private ConcurrentMap<Integer, WebUiGroup> uiGroups;
    private ConcurrentMap<Integer, WebUiPage> uiPages;

    private ConfigManager() {
        this.config = new ConcurrentHashMap<String, Config>();
        this.uiGroups = new ConcurrentHashMap<Integer, WebUiGroup>();
        this.uiPages = new ConcurrentHashMap<Integer, WebUiPage>();
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
    boolean registerConfig( Config config) {
        if( validateConfig( config )) {
            if ( this.config.get( config.getKey() ) != null ) {
                this.config.get( config.getKey() ).override( config );
            }else {
                this.config.put( config.getKey(),config );
            }
            //setChanged();
            //notifyObservers( this.config );
            return true;
        } else {
            System.out.println( "did not add "+config.getKey()+" because keyname too long" );
            return false;
        }
    }

    /**
     * @param key (unique) key of the configuration
     * */
    Object getObject ( String key) {
        return config.get( key ).getValue();
    }

    /** get configuration as int */
    int getInt ( String key ) {
        return (int) config.get( key ).getValue();
    }

    /** get configuration as String*/
    String getString ( String key ) {
        return (String) config.get( key ).getValue();
    }

    /** get configuration as Configuration object */
    Config getConfig( String s ) {
        return config.get( s );
    }

    //todo throw exception if config does not exist
    /** change the value of a configuration in the ConfigManager
     * @param key key of the configuration
     * @param value new value for the configuration */
    boolean setConfigValue( String key, Object value ) {
        if( config.get( key ) != null){
            config.get( key ).setValue( value );
            return true;
        } else {
            return false;
        }
    }

    /** dummy validation method for testing  */
    private boolean validateConfig ( Config c ) {
        return c.getKey().length() <= 100;
    }

    /** add a WebUiGroup to the ConfigManager
     * @param g WebUiGroup to add */
    void addUiGroup ( WebUiGroup g ) {
        if ( this.uiGroups.get( g.getId() ) != null ){
            this.uiGroups.get( g.getId() ).override( g );
        } else {
            this.uiGroups.put( g.getId(), g );
        }
    }

    /** add a WebUiPage to the ConfigManager
     * @param p WebUiPage to add */
    void addUiPage ( WebUiPage p ) {
        if ( this.uiPages.get( p.getId() ) != null ) {
            this.uiPages.get( p.getId() ).override( p );
        } else {
            this.uiPages.put( p.getId(), p );
        }
    }

    String getPageList () {
        //fill WebUiGroups with Configs
        //todo getPageList()
        return "TODO";
    }

    /** get certain page as json
     * @param id pageId */
    //todo sort
    String getPage ( int id ) {
        //fill WebUiGroups with Configs
        for( ConcurrentMap.Entry<String, Config> c : config.entrySet()){
            try{
                int i = c.getValue().getWebUiGroup();
                this.uiGroups.get( i ).addConfig( c.getValue() );
            } catch ( NullPointerException e ){
                System.out.println("skipping config "+c.getKey()+" with no WebUiGroup");
            }
        }

        //fill WebUiPages with WebUiGroups
        for( ConcurrentMap.Entry<Integer, WebUiGroup> g : uiGroups.entrySet() ){
            try{
                int i = g.getValue().getPageId();
                this.uiPages.get( i ).addWebUiGroup( g.getValue() );
            } catch ( NullPointerException e ){
                System.out.println("skipping group "+g.getKey()+" with no pageid");
            }
        }
        return uiPages.get( id ).toString();
    }
}
