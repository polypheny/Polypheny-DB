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


import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import spark.Request;
import spark.utils.IOUtils;


public class Resource {

    public ResponseCreator get( final Request _req ) {

        String path = _req.splat()[0];

        InputStream stream = null;
        try {
            JarFile file = new JarFile( this.getClass().getClassLoader().getResource( "jar/Polypheny-DB-UI-1.0-SNAPSHOT.jar" ).getFile() );
            JarEntry entry = file.getJarEntry( "static/" + path );
            stream = file.getInputStream( entry );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        if ( stream != null ) {
            try {
                return MyResponse.ok( IOUtils.toString( stream ), path );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
        }

        //else:
        return MyResponse.badRequest( "Did not find File " + path + " in UI jar File." );

    }
}
