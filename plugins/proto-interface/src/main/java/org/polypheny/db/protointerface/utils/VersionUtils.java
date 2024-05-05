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

package org.polypheny.db.protointerface.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.Getter;

public class VersionUtils {

    @Getter
    private static final int MAJOR_API_VERSION;
    @Getter
    private static final int MINOR_API_VERSION;


    static {
        Properties prop = new Properties();
        try ( InputStream input = VersionUtils.class.getClassLoader().getResourceAsStream( "version.properties" ) ) {
            prop.load( input );
            MAJOR_API_VERSION = Integer.parseInt( prop.getProperty( "MAJOR_API_VERSION" ) );
            MINOR_API_VERSION = Integer.parseInt( prop.getProperty( "MINOR_API_VERSION" ) );
        } catch ( IOException ex ) {
            throw new RuntimeException( "Failed to load version properties", ex );
        }
    }

}
