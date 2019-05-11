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


import static spark.Spark.get;
import static spark.Spark.port;

import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.utils.IOUtils;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger( Server.class );

    public static void main( String[] args ) {
        Server s = new Server();
    }

    public Server() {

        port( 81 );

        //staticFiles.location( "jar" );
        //Spark.staticFileLocation( "jar" );
        Resource resource = new Resource();

        get( "/", ( req, res ) -> {
            JarFile file = new JarFile( this.getClass().getClassLoader().getResource( "jar/Polypheny-DB-UI-1.0-SNAPSHOT.jar" ).getFile() );
            JarEntry entry = file.getJarEntry( "static/index.html" );
            InputStream stream = file.getInputStream( entry );

            return IOUtils.toString( stream );
        } );

        //source of mapping idea:
        //https://www.deadcoderising.com/sparkjava-separating-routing-and-resources/
        get( "/*", map( ( req, res ) -> resource.get( req ) ) );

        LOGGER.info( "HTTP Server started." );

    }

    Route map( final Converter c ) {
        return ( req, res ) -> c.convert( req, res ).handle( req, res );
    }


    private interface Converter {

        ResponseCreator convert( Request req, Response res );
    }

}
