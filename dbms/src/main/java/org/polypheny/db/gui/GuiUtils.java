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

package org.polypheny.db.gui;

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.config.RuntimeConfig;


@Slf4j
public class GuiUtils {

    public static String getPolyphenyVersion() {
        String v = PolyphenyDb.class.getPackage().getImplementationVersion();
        if ( v == null ) {
            return "Unknown";
        } else {
            return v;
        }
    }


    public static String getBuildDate() {
        URL jarURL = PolyphenyDb.class.getClassLoader().getResource( "org/polypheny/db/PolyphenyDb.class" );
        try {
            if ( jarURL.openConnection() instanceof JarURLConnection ) {
                JarURLConnection jurlConn = (JarURLConnection) jarURL.openConnection();
                Manifest mf = jurlConn.getManifest();
                Attributes attr = mf.getMainAttributes();
                return attr.getValue( "Build-Date" );
            }
            return "Unknown";
        } catch ( Exception e ) {
            return "Unknown";
        }
    }


    public static boolean checkPolyphenyAlreadyRunning() {
        try {
            String url = "http://localhost:" + RuntimeConfig.WEBUI_SERVER_PORT.getInteger() + "/product";
            return httpGetRequest( url ).equals( "Polypheny-DB" );
        } catch ( Exception e ) {
            return false;
        }
    }


    private static String httpGetRequest( String urlToRead ) throws java.io.IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL( urlToRead );
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod( "GET" );
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( conn.getInputStream() ) ) ) {
            for ( String line; (line = reader.readLine()) != null; ) {
                result.append( line );
            }
        }
        return result.toString().trim();
    }


    public static void openUiInBrowser() {
        try {
            Desktop.getDesktop().browse( new URI( "http://localhost:" + RuntimeConfig.WEBUI_SERVER_PORT.getInteger() ) );
        } catch ( IOException | URISyntaxException ex ) {
            log.error( "Exception while opening UI in browser.", ex );
        }
    }

}
