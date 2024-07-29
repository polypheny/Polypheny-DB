/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.prisminterface.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.Getter;

public class VersionUtils {

    private static final String API_VERSION_PROPERTIES = "prism-api-version.properties";

    @Getter
    private static final int MAJOR_API_VERSION;
    @Getter
    private static final int MINOR_API_VERSION;
    @Getter
    private static final String API_VERSION_STRING;


    static {
        Properties properties = new Properties();
        try ( InputStream inputStream = VersionUtils.class.getClassLoader().getResourceAsStream( API_VERSION_PROPERTIES ) ) {
            if ( inputStream != null ) {
                properties.load( inputStream );
                API_VERSION_STRING = properties.getProperty( "version" );
                MAJOR_API_VERSION = Integer.parseInt( properties.getProperty( "majorVersion" ) );
                MINOR_API_VERSION = Integer.parseInt( properties.getProperty( "minorVersion" ) );
            } else {
                throw new FileNotFoundException( "The prism api version properties could not be found." );
            }
        } catch ( IOException e ) {
            throw new RuntimeException( "Error loading API version properties", e );
        }
    }
}
