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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;

public class ClientConfiguration {

    public static final boolean DEFAULT_AUTOCOMMIT = true;
    public static final boolean DEFAULT_STREAM_ALL = false;
    public static final String DEFAULT_NAMESPACE_NAME = Catalog.DEFAULT_NAMESPACE_NAME;
    public static final int DEFAULT_FETCH_SIZE = 100;
    public static final int DEFAULT_MESSAGE_SIZE = 1000000000;

    public static final String SERVER_STREAMING_FEATURE_KEY = "server_streaming";
    public static final String CLIENT_STREAMING_FEATURE_KEY = "client_streaming";

    public static final String AUTOCOMMIT_PROPERTY_KEY = "autocommit";
    public static final String STREAM_ALL_PROPERTY_KEY = "stream_all";
    public static final String DEFAULT_NAMESPACE_PROPERTY_KEY = "namespace";
    public static final String PREFERRED_MESSAGE_SIZE_PROPERTY_KEY = "preferred_message_size";
    public static final String FETCH_SIZE_PROPERTY_KEY = "fetch_size";


    private static final Set<String> availableFeatures = new HashSet<>( List.of(
            SERVER_STREAMING_FEATURE_KEY,
            CLIENT_STREAMING_FEATURE_KEY
    ) );

    private static final Set<String> availableProperties = new HashSet<>( List.of(
            AUTOCOMMIT_PROPERTY_KEY,
            STREAM_ALL_PROPERTY_KEY,
            PREFERRED_MESSAGE_SIZE_PROPERTY_KEY,
            FETCH_SIZE_PROPERTY_KEY
    ) );

    private final Set<String> supportedFeatures;
    private final Map<String, String> properties;


    public ClientConfiguration() {
        this.supportedFeatures = new HashSet<>();
        this.properties = new HashMap<>();
    }


    public Set<String> addFeatures( List<String> clientFeatures ) {
        Map<Boolean, List<String>> partitionedFeatures = clientFeatures.stream()
                .collect( Collectors.partitioningBy( availableFeatures::contains ) );
        supportedFeatures.addAll( partitionedFeatures.get( true ) );
        return new HashSet<>( partitionedFeatures.get( false ) );
    }

    public void setDefaultProperties() {
        properties.put( AUTOCOMMIT_PROPERTY_KEY, String.valueOf(DEFAULT_AUTOCOMMIT) );
        properties.put( DEFAULT_NAMESPACE_PROPERTY_KEY, DEFAULT_NAMESPACE_NAME );
        properties.put( FETCH_SIZE_PROPERTY_KEY, String.valueOf(DEFAULT_FETCH_SIZE));
        properties.put( PREFERRED_MESSAGE_SIZE_PROPERTY_KEY, String.valueOf(DEFAULT_MESSAGE_SIZE));
        properties.put( STREAM_ALL_PROPERTY_KEY, String.valueOf(DEFAULT_STREAM_ALL));
    }

    public Set<String> setProperties( Map<String, String> clientProperties ) {
        Map<Boolean, List<Map.Entry<String, String>>> partitionedProperties = clientProperties.entrySet().stream()
                .collect( Collectors.partitioningBy( entry -> availableProperties.contains( entry.getKey() ) ) );
        partitionedProperties.get( true ).forEach( entry -> properties.put( entry.getKey(), entry.getValue() ) );
        return partitionedProperties.get( false ).stream()
                .map( Map.Entry::getKey )
                .collect( Collectors.toSet() );
    }

    public boolean isSupported( String featureName ) {
        return supportedFeatures.contains( featureName );
    }

    public String getProperty( String propertyName ) {
        return properties.get( propertyName );
    }

}
