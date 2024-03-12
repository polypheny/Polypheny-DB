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

package org.polypheny.db.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Getter
public class VersionCollector {

    public static final String VERSION = "version";
    public static final String BRANCH = "branch";
    public static final String HASH = "hash";

    public static VersionCollector INSTANCE = new VersionCollector();

    @Getter(AccessLevel.NONE) // Do not create a Getter
    private final Properties versionProperties = new Properties();

    private final String version;
    private final String branch;
    private final String hash;

    private boolean inJar = false;


    private VersionCollector() {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream( "version.properties" );

        try {
            if ( inputStream == null ) {
                inJar = true;
                // When running unit tests, no jar is built, so we load a copy of the file that we saved during build.gradle.
                // Possibly this also is the case during debugging, therefore we save in bin/main instead of bin/test.
                inputStream = new FileInputStream( "version.properties" );
            }
            versionProperties.load( inputStream );
        } catch ( Exception e ) {
            useDefaults();
        }

        version = versionProperties.getProperty( VERSION );
        branch = versionProperties.getProperty( BRANCH );
        hash = versionProperties.getProperty( HASH );
    }


    private void useDefaults() {
        log.warn( "Using default version." );
        versionProperties.put( VERSION, "default" );
        versionProperties.put( BRANCH, "default" );
    }

}
