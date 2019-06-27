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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Information Manager manages information objects, Information Groups and Information Pages
 */
public class InformationManager {

    private static final Logger LOG = LoggerFactory.getLogger( InformationManager.class );

    private static final String MAIN_MANAGER_IDENTIFIER = "0";

    /**
     * Map of instances.
     */
    private static ConcurrentMap<String, InformationManager> instances = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Information> informationMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InformationPage> pages = new ConcurrentHashMap<>();

    private ConcurrentLinkedQueue<InformationObserver> observers = new ConcurrentLinkedQueue<>();

    /**
     *  Identifier of this information manager. null for the main information manger.
     */
    private final String instanceId;


    private InformationManager(final String instanceId) {
        // private constructor to ensure singleton is applied by calling get instance
        this.instanceId = instanceId;
    }


    /**
     * Singleton.
     * Without a id the main Information Manager is returned.
     */
    public static InformationManager getInstance() {
        return getInstance( MAIN_MANAGER_IDENTIFIER );
    }


    /**
     * Singleton.
     * null returns the main information manager.
     */
    public static InformationManager getInstance( final String instanceId ) {
        if ( !instances.containsKey( instanceId ) ) {
            instances.put( instanceId, new InformationManager( instanceId ) );
        }
        return instances.get( instanceId );
    }


    /**
     * Close an information manager.
     */
    public static void close( final String id ) {
        if ( id.equals( MAIN_MANAGER_IDENTIFIER ) ) {
            throw new RuntimeException( "It is not allowed to close the main Information Manager" );
        }
        instances.remove( id );
    }


    /**
     * Add a WebUI page to the Information Manager.
     *
     * @param page Page to add
     */
    public void addPage( final InformationPage page ) {
        if( this.pages.containsKey( page.getId() )){
            InformationPage existing = this.pages.get( page.getId() );
            if( !existing.isImplicit() ){
                throw new InformationRuntimeException( "You are trying to add an InformationPage twice to the InformationManager." );
            } else{
                existing.overrideWith( page );
            }
        } else {
            this.pages.put( page.getId(), page );
            this.notifyPageList();
        }
    }


    /**
     * Add one or multiple WebUI groups to the Information Manager.
     *
     * @param groups Groups to add
     */
    public void addGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            if( this.groups.containsKey( g.getId() )){
                InformationGroup existing = this.groups.get( g.getId() );
                if( !existing.isImplicit() ){
                    throw new InformationRuntimeException( "You are trying to add an InformationGroup twice to the InformationManager" );
                } else {
                    existing.overrideWith( g );
                }
            } else {
                this.groups.put( g.getId(), g );
            }
        }
    }


    /**
     * Add a QueryPlan to the Information Manager and create the needed page and group implicitly.
     */
    public void addQueryPlan ( final String id, final String queryPlan ) {
        InformationPage page = new InformationPage( id );
        InformationGroup group = new InformationGroup( id, id ).setImplicit( true );
        InformationQueryPlan plan = new InformationQueryPlan( id, id, queryPlan );

        this.addPage( page );
        this.addGroup( group );
        this.informationMap.put( plan.getId(), plan );
    }


    /**
     * Register one or multiple Information objects in the Information Manager.
     *
     * @param infos Information objects to register
     */
    public void registerInformation( final Information... infos ) {
        for ( Information i : infos ) {
            this.informationMap.put( i.getId(), i.setManager( this ) );
        }
    }


    /**
     * Remove one or multiple Information Objects from the Information Manager.
     *
     * @param infos Information Object to remove
     */
    public void removeInformation( final Information... infos ) {
        for ( Information i : infos ) {
            this.informationMap.remove( i.getId(), i );
        }
    }


    /**
     * Get the Information object with a certain key.
     *
     * @param key of the Information object that should be returned
     * @return Information object with key <i>key</i>
     * @throws InformationRuntimeException If there is no information element with that key
     */
    public Information getInformation( final String key ) {
        if ( informationMap.containsKey( key ) ) {
            return this.informationMap.get( key );
        } else {
            throw new InformationRuntimeException( "There is no information element registered with that key: " + key );
        }
    }


    /**
     * Returns the list of pages of the Information Manager as JSON using GSON.
     *
     * @return List of pages of the Information Manager as JSON
     */
    public String getPageList() {
        InformationPage[] pages1 = new InformationPage[this.pages.size()];
        int counter = 0;
        for ( InformationPage p : this.pages.values() ) {
            pages1[counter] = p;
            counter++;
        }
        Gson gson = new Gson();
        return gson.toJson( pages1, InformationPage[].class );
    }


    /**
     * Get a page from the Information Manager with a certain id.
     *
     * @param id The id of the page that should be returned
     * @return the requested InformationPage
     */
    public InformationPage getPage( final String id ) {
        InformationPage p = this.pages.get( id );

        for ( Information i : this.informationMap.values() ) {
            String group = i.getGroup();
            this.groups.get( group ).addInformation( i );
        }

        for ( InformationGroup g : this.groups.values() ) {
            String page = g.getPageId();
            this.pages.get( page ).addGroup( g );
        }
        return p;
    }


    /**
     * Add observer to the list of observers. The caller needs to provide an id as well.
     */
    public InformationManager observe( final InformationObserver observer ) {
        this.observers.add( observer );
        return this;
    }


    /**
     * Send an updated information object as JSON via Websocket to the WebUI
     */
    public void notify( final Information i ) {
        for( InformationObserver observer : this.observers ){
            observer.observeInfos( i );
        }
    }


    private void notifyPageList (){
        for( InformationObserver observer : this.observers ){
            observer.observePageList( instanceId, this.pages.values().toArray( new InformationPage[0] ) );
        }
    }

}
