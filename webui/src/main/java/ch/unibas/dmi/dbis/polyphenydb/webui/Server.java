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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
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

        extractJar();

        port( 81 );

        //Spark.staticFileLocation( "jar/ui" );
        Spark.staticFiles.location( "jar/ui/static" );

        get( "/", ( req, res ) -> {
            JarFile file = new JarFile( this.getClass().getClassLoader().getResource( "jar/Polypheny-DB-UI-1.0-SNAPSHOT.jar" ).getFile() );
            JarEntry entry = file.getJarEntry( "static/index.html" );
            InputStream stream = file.getInputStream( entry );

            return IOUtils.toString( stream );
        } );


        LOGGER.info( "HTTP Server started." );

    }

    /**
     * extracts the JAR containing the WebUI files, such that the Spark server can serve them using staticFiles
     */
    //source: https://stackoverflow.com/questions/1529611/how-to-write-a-java-program-which-can-extract-a-jar-file-and-store-its-data-in-s
    private void extractJar() {
        final String destDir = this.getClass().getClassLoader().getResource( "jar" ).getPath() + "/ui";
        final String jarPath = "jar/Polypheny-DB-UI-1.0-SNAPSHOT.jar";
        try {
            try {
                deleteDirectory( new File( this.getClass().getClassLoader().getResource( "jar/ui" ).getFile() ) );
                System.out.println( "Deleted folder jar/ui to create anew." );
            } catch ( NullPointerException e ) {
                //can skip, don't have to delete if not existing yet
            }

            System.out.println( "Starting to extract jar..." );
            System.out.println( "This may take a minute." );
            JarFile jar = new JarFile( this.getClass().getClassLoader().getResource( jarPath ).getFile() );
            java.util.Enumeration enumEntries = jar.entries();
            while ( enumEntries.hasMoreElements() ) {
                JarEntry file = (java.util.jar.JarEntry) enumEntries.nextElement();
                File f = new File( destDir + java.io.File.separator + file.getName() );
                if ( file.isDirectory() ) { // if its a directory, create it
                    boolean dirCreated = f.mkdirs();
                    if ( !dirCreated ) {
                        System.err.println( "Could not create directory " + f.getName() );
                        break;
                    }
                    continue;
                }
                java.io.InputStream is = jar.getInputStream( file ); // get the input stream
                java.io.FileOutputStream fos = new java.io.FileOutputStream( f );
                while ( is.available() > 0 ) {  // write contents of 'is' to 'fos'
                    fos.write( is.read() );
                }
                fos.close();
                is.close();
            }
            jar.close();
            System.out.println( "Finished extracting jar." );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }


    /**
     * Delete an existing directory.
     */
    //source: https://softwarecave.org/2018/03/24/delete-directory-with-contents-in-java/
    private void deleteDirectory( final File file ) throws IOException {
        if ( file.isDirectory() ) {
            File[] entries = file.listFiles();
            if ( entries != null ) {
                for ( File entry : entries ) {
                    deleteDirectory( entry );
                }
            }
        }
        if ( !file.delete() ) {
            throw new IOException( "Failed to delete " + file );
        }
    }

}
