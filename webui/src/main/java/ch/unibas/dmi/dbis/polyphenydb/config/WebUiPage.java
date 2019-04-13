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
    private ConcurrentMap<String, WebUiGroup> groups = new ConcurrentHashMap<String, WebUiGroup>(  );


    /**
     * @param id unique id for the page
     */
    public WebUiPage( String id ) {
        this.id = id;
    }


    public WebUiPage ( String id, String title, String description ) {
        this.id = id;
        this.title = title;
        this.description = description;
    }


    public WebUiPage withIcon( String icon ) {
        this.icon = icon;
        return this;
    }


    public WebUiPage withParent( WebUiPage parent ) {
        this.parentPage = parent;
        return this;
    }


    public boolean hasTitle() {
        return this.title != null;
    }


    /**
     * @return id of this WebUiPage
     */
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
     * add a WebUiGroup for this WebUiPage
     */
    public void addWebUiGroup( WebUiGroup g ) {
        groups.put( g.getId(), g );
    }


    /**
     * @return returns WebUiPage as json object
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
