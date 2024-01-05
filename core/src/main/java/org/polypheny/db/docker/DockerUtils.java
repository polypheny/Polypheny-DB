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

package org.polypheny.db.docker;

import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.models.DockerHost;

public class DockerUtils {

    static final String CONTAINER_NAME = "polypheny-docker-connector";
    static final String IMAGE_NAME = "polypheny/polypheny-docker-connector";
    static final String VOLUME_NAME = "polypheny-docker-connector-data";


    private DockerUtils() {
    }


    public static String normalizeHostname( String hostname ) {
        // TODO: add more validation/sanity checks
        String newHostname = hostname.strip();
        if ( newHostname.isEmpty() ) {
            throw new GenericRuntimeException( "invalid hostname \"" + newHostname + "\"" );
        }
        return newHostname;
    }

    public static String getContainerName( DockerHost host ) {
        final String registryToUse = host.getRegistryOrDefault();
        if ( registryToUse.isEmpty() || registryToUse.endsWith( "/" ) ) {
            return registryToUse + IMAGE_NAME;
        } else {
            return registryToUse + "/" + IMAGE_NAME;
        }
    }

}
