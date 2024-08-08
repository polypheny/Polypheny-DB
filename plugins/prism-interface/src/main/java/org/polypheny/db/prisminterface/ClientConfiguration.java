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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;

public class ClientConfiguration {

    public static final boolean DEFAULT_AUTOCOMMIT = true;
    public static final String DEFAULT_NAMESPACE_NAME = Catalog.DEFAULT_NAMESPACE_NAME;
    public static final int DEFAULT_FETCH_SIZE = 100;

    public static final String SERVER_STREAMING = "server_streaming";
    public static final String CLIENT_STREAMING = "client_streaming";

    private static final Set<String> availableFeatures = new HashSet<>( List.of(
            SERVER_STREAMING,
            CLIENT_STREAMING
    ) );

    @Getter
    private final Set<String> supportedFeatures;


    public ClientConfiguration() {
        this.supportedFeatures = new HashSet<>();
    }


    public Set<String> addFeatures( List<String> clientFeatures ) {
        Map<Boolean, List<String>> partitionedFeatures = clientFeatures.stream()
                .collect( Collectors.partitioningBy( availableFeatures::contains ) );
        supportedFeatures.addAll( partitionedFeatures.get( true ) );
        return new HashSet<>( partitionedFeatures.get( false ) );
    }


    public boolean isSupported( String featureName ) {
        return supportedFeatures.contains( featureName );
    }

}
