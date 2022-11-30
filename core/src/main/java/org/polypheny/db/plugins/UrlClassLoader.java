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

package org.polypheny.db.plugins;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;

public class UrlClassLoader extends URLClassLoader {

    static {
        registerAsParallelCapable();
    }


    /*
     * Required when this classloader is used as the system classloader.
     */
    public UrlClassLoader( ClassLoader parent ) {
        super( new URL[0], parent );
    }


    @Override
    public void addURL( URL url ) {
        super.addURL( url );
    }


    public void addFile( File file ) throws IOException {
        addURL( file.getCanonicalFile().toURI().toURL() );
    }


    /*
     *  Required for Java Agents when this classloader is used as the system classloader.
     */
    @SuppressWarnings("unused")
    private void appendToClassPathForInstrumentation( String jarPath ) throws IOException {
        addURL( Paths.get( jarPath ).toRealPath().toUri().toURL() );
    }


}
