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

package ch.unibas.dmi.dbis.polyphenydb.webui;


import com.google.gson.Gson;
import java.util.Random;
import spark.Request;
import spark.Response;


/**
 * Create, read, update and delete elements from a database
 * contains only demo data so far
 */
class Crud {

    /**
     * returns a demo table
     */
    String getTable( final Request req, final Response res ) {
        Gson gson = new Gson();
        UIRequest request = gson.fromJson( req.body(), UIRequest.class );
        String[] header = { "a", "b", "c" };
        Random random = new Random();
        Integer[][] data = new Integer[2][3];
        for ( int i = 0; i < data.length; i++ ) {
            for ( int j = 0; j < data[0].length; j++ ) {
                data[i][j] = random.nextInt( 100 );
            }
        }
        return new ResultSet<>( header, data ).setCurrentPage( request.currentPage ).setHighestPage( 20 ).toJson();
    }


    /**
     * returns a demo Tree with information about a Database
     */
    String getSchemaTree( final Request req, final Response res ) {
        SidebarElement db = new SidebarElement( "db", "db" );
        SidebarElement t1 = new SidebarElement( "db.t1", "t1" );
        SidebarElement t2 = new SidebarElement( "db.t2", "t2" );
        SidebarElement db2 = new SidebarElement( "db2", "db2" );
        SidebarElement t3 = new SidebarElement( "db.t3", "t3" );
        SidebarElement t4 = new SidebarElement( "db.t4", "t4" );
        db.setChildren( t1, t2 );
        db2.setChildren( t3, t4 );
        SidebarElement[] result = { db, db2 };
        Gson gson = new Gson();
        return gson.toJson( result );
    }

    /**
     * Contains data from a query, the titles of the columns and information about the pagination
     */
    class ResultSet<T> {

        String[] header;
        T[][] data;
        int currentPage;
        int highestPage;

        public ResultSet( final String[] header, final T[][] data ) {
            this.header = header;
            this.data = data;
        }

        public String toJson() {
            Gson gson = new Gson();
            return gson.toJson( this );
        }

        ResultSet setCurrentPage( final int page ) {
            this.currentPage = page;
            return this;
        }

        ResultSet setHighestPage( final int highestPage ) {
            this.highestPage = highestPage;
            return this;
        }

    }


    /**
     * can be used to define data for the left sidebar in the UI
     */
    class SidebarElement {

        String id;
        String name;
        String icon;//todo: enum
        String routerLink;
        SidebarElement[] children;

        SidebarElement( final String id, final String name ) {
            this.id = id;
            this.name = name;
            this.routerLink = "/views/data-table/" + id;
        }

        SidebarElement( final String id, final String name, final String icon ) {
            this( id, name );
            this.icon = icon;
        }

        void setChildren( final SidebarElement... children ) {
            this.children = children;
        }
    }


    /**
     * needed to parse a request coming from the UI using Gson
     */
    class UIRequest {

        String tableId;
        int currentPage;
    }

}
