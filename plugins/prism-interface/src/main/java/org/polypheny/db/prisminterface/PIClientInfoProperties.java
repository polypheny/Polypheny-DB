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

package org.polypheny.db.prisminterface;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

class PIClientInfoProperties extends Properties {

    private static final int MAX_STRING_LENGTH = 2147483647;
    static final List<ClientInfoPropertiesDefault> DEFAULTS = Arrays.asList(
            new ClientInfoPropertiesDefault(
                    "ApplicationName",
                    "",
                    MAX_STRING_LENGTH,
                    "Name of the application which interacts with the prism-interface by this client."
            ),
            new ClientInfoPropertiesDefault(
                    "ApplicationVersionString",
                    "",
                    MAX_STRING_LENGTH,
                    "Version description of the application that interacts with the prism-interface through this user."
            ),
            new ClientInfoPropertiesDefault(
                    "ClientHostname",
                    "",
                    MAX_STRING_LENGTH,
                    "Hostname of the computer on which the application is running that interacts with the prism-interface via this user."
            ),
            new ClientInfoPropertiesDefault(
                    "ClientUser",
                    "",
                    MAX_STRING_LENGTH,
                    "User name of the user under which the application interacting with the prism-interface via this user is running."
            )
    );


    PIClientInfoProperties() {
        super();
        DEFAULTS.forEach( p -> setProperty( p.key, p.default_value ) );
    }


    record ClientInfoPropertiesDefault( String key, String default_value, int maxLength, String description ) {

    }

}
