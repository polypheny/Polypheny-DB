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

package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import com.google.gson.Gson;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class InformationManager {

    private static InformationManager instance;
    private ConcurrentMap<String, Information> informationMap = new ConcurrentHashMap<String, Information>();
    private ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<String, InformationGroup>();
    private ConcurrentMap<String, InformationPage> pages = new ConcurrentHashMap<String, InformationPage>();


    private InformationManager() {
    }


    /**
     * singleton pattern
     */
    public static InformationManager getInstance() {
        if ( instance == null ) {
            instance = new InformationManager();
        }
        return instance;
    }


    public void addPage( InformationPage p ) {
        this.pages.put( p.getId(), p );
    }


    public void addGroup( InformationGroup... groups ) {
        for ( InformationGroup g : groups ) {
            this.groups.put( g.getId(), g );
        }
    }


    public void registerInformation( Information... infos ) {
        for ( Information i : infos ) {
            this.informationMap.put( i.getId(), i );
        }
    }


    public void removeInformation( Information... infos ) {
        for ( Information i : infos ) {
            this.informationMap.remove( i.getId(), i );
        }
    }


    public Information getInformation( String key ) {
        return this.informationMap.get( key );
    }


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


    public String getPage( String id ) {
        InformationPage p = this.pages.get( id );

        for ( Information i : this.informationMap.values() ) {
            String group = i.getGroup();
            this.groups.get( group ).addInformation( i );
        }

        for ( InformationGroup g : this.groups.values() ) {
            String page = g.getPageId();
            this.pages.get( page ).addGroup( g );
        }
        //System.out.println( p.toString() );
        return p.toString();
    }


    public void notify( Information i ) {
        try {
            InformationWebSocket.broadcast( i.toString() );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

}
