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

package org.polypheny.db.information;


import com.google.gson.Gson;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.information.exception.InformationRuntimeException;


/**
 * The Information Manager manages information objects, Information Groups and Information Pages
 */
@Slf4j
public class InformationManager {

    private static final String MAIN_MANAGER_IDENTIFIER = "0";

    /**
     * Map of instances.
     */
    private static final ConcurrentMap<String, InformationManager> instances = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Information> informationMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InformationPage> pages = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<InformationObserver> observers = new ConcurrentLinkedQueue<>();

    /**
     * WebsocketConnection for notifications
     */
    @Setter
    private Session session = null;

    /**
     * Identifier of this information manager.
     */
    private final String instanceId;


    private InformationManager( final String instanceId ) {
        // private constructor to ensure singleton is applied by calling get instance
        this.instanceId = instanceId;

        // Add the information page about the information manager itself
        if ( instanceId.equals( MAIN_MANAGER_IDENTIFIER ) ) {
            InformationPage page = new InformationPage(
                    "InformationManager",
                    "Information Manager",
                    "Information about the information manager itself." );
            page.fullWidth();
            this.addPage( page );
            // Running instances
            InformationGroup runningInstancesGroup = new InformationGroup( page, "Instances" );
            this.addGroup( runningInstancesGroup );
            InformationTable runningInstancesTable = new InformationTable(
                    runningInstancesGroup,
                    Arrays.asList( "ID", "Pages", "Groups", "Elements", "Observers" ) );
            this.registerInformation( runningInstancesTable );
            page.setRefreshFunction( () -> {
                runningInstancesTable.reset();
                instances.forEach( ( k, v ) -> runningInstancesTable.addRow( k.substring( 0, Math.min( k.length(), 8 ) ), v.pages.size(), v.groups.size(), v.informationMap.size(), v.observers.size() ) );
            } );
        }
    }


    /**
     * Singleton.
     * Without an id the main Information Manager is returned.
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
        InformationManager im = instances.remove( id );
        if ( im != null ) {
            im.observers.forEach( im.observers::remove );
        }
    }


    /**
     * Add a WebUI page to the Information Manager.
     *
     * @param page Page to add
     */
    public void addPage( final InformationPage page ) {
        if ( this.pages.containsKey( page.getId() ) ) {
            InformationPage existing = this.pages.get( page.getId() );
            if ( !existing.isImplicit() ) {
                throw new InformationRuntimeException( "You are trying to add an InformationPage twice to the InformationManager." );
            } else {
                existing.overrideWith( page );
            }
        } else {
            this.pages.put( page.getId(), page );
            this.notifyPageList();
        }
    }


    /**
     * Deregister a information page.
     *
     * @param page Page tp remove
     */
    public void removePage( final InformationPage page ) {
        if ( this.pages.containsKey( page.getId() ) ) {
            this.pages.remove( page.getId() );
        } else {
            log.warn( "Trying to remove a information page which is not registered in this information manager." );
        }
    }


    /**
     * Add one or multiple WebUI groups to the Information Manager.
     *
     * @param groups Groups to add
     */
    public void addGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            if ( this.groups.containsKey( g.getId() ) ) {
                InformationGroup existing = this.groups.get( g.getId() );
                if ( !existing.isImplicit() ) {
                    throw new InformationRuntimeException( "You are trying to add an InformationGroup twice to the InformationManager." );
                } else {
                    existing.overrideWith( g );
                }
            } else {
                this.groups.put( g.getId(), g );
            }
        }
    }


    /**
     * Deregister one or multiple information groups.
     *
     * @param groups Groups to remove
     */
    public void removeGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            if ( this.groups.containsKey( g.getId() ) ) {
                this.groups.remove( g.getId() );
                getPage( g.getPageId() ).removeGroup( g );
            } else {
                log.warn( "Trying to remove a information group which is not registered in this information manager." );
            }
        }
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
            this.informationMap.remove( i.getId() );
            getGroup( i.getGroup() ).removeInformation( i );
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
        Arrays.sort( pages1, Comparator.comparing( InformationPage::getName ) );
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
            g.refresh();
            String page = g.getPageId();
            this.pages.get( page ).addGroup( g );
        }

        // p can be null if the client requests a unknown page. This often happens after a restart of Polypheny-DB if a
        // certain information page is not (yet) available.
        if ( p != null ) {
            p.refresh();
        }
        return p;
    }


    /**
     * Get an InformationGroup WITHOUT its InformationObjects
     */
    public InformationGroup getGroup( final String id ) {
        return this.groups.get( id );
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
        String info = i.asJson();
        for ( InformationObserver observer : this.observers ) {
            observer.observeInfos( info, instanceId, session );
        }
    }


    private void notifyPageList() {
        for ( InformationObserver observer : this.observers ) {
            observer.observePageList( this.pages.values().toArray( new InformationPage[0] ), instanceId, session );
        }
    }

}
