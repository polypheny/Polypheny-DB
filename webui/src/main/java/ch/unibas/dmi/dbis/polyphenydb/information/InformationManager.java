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
import ch.unibas.dmi.dbis.polyphenydb.webui.InformationWebSocket;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InformationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger( InformationManager.class );

    private static InformationManager instance;

    private final ConcurrentMap<String, Information> informationMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InformationPage> pages = new ConcurrentHashMap<>();


    private InformationManager() {
        // empty and private constructor to ensure singleton is applied by calling get instance
    }


    /**
     * Singleton
     */
    public static InformationManager getInstance() {
        if ( instance == null ) {
            instance = new InformationManager();
        }
        return instance;
    }


    /**
     * Add a WebUI page to the InformationManager
     *
     * @param page page to add
     */
    public void addPage( final InformationPage page ) {
        this.pages.put( page.getId(), page );
    }


    /**
     * Add one or multiple WebUI groups to the InformationManager
     *
     * @param groups groups to add
     */
    public void addGroup( final InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            this.groups.put( g.getId(), g );
        }
    }


    /**
     * Register one or multiple Information objects in the InformationManager
     *
     * @param infos Information objects to register
     */
    public void registerInformation( final Information... infos ) {
        for ( Information i : infos ) {
            this.informationMap.put( i.getId(), i );
        }
    }


    /**
     * Remove one or multiple Information Object from the InformationManager
     *
     * @param infos Information Object to remove
     */
    public void removeInformation( final Information... infos ) {
        for ( Information i : infos ) {
            this.informationMap.remove( i.getId(), i );
        }
    }


    /**
     * Get the Information object with a certain key
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
     * Returns the list of pages of the Information Manager as JSON using Gson
     *
     * @return list of pages of the Information Manager as JSON
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
     * Get a certain page as JSON using Gson
     *
     * @param id The id of the page that should be returned
     * @return page as JSON string
     */
    public String getPage( final String id ) {
        InformationPage p = this.pages.get( id );

        for ( Information i : this.informationMap.values() ) {
            String group = i.getGroup();
            this.groups.get( group ).addInformation( i );
        }

        for ( InformationGroup g : this.groups.values() ) {
            String page = g.getPageId();
            this.pages.get( page ).addGroup( g );
        }
        return p.asJson();
    }


    /**
     * Send an updated information object as JSON via Websocket to the WebUI
     */
    public void notify( final Information i ) {
        try {
            InformationWebSocket.broadcast( i.asJson() );
        } catch ( IOException e ) {
            LOGGER.info( "Error while sending information object to web ui!", e );
        }
    }

}
