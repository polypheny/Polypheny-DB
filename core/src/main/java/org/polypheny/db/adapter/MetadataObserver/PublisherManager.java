/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.MetadataObserver;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PublisherManager {

    private final Map<String, MetadataPublisher> publishers = new ConcurrentHashMap<>();

    private static final PublisherManager INSTANCE = new PublisherManager();

    public static PublisherManager getInstance() {
        return INSTANCE;
    }

    private PublisherManager() {
    }


    public <P extends Adapter & MetadataProvider> void onAdapterDeploy( P adapter ) {
        log.info( "Adapter {} is going to be registered for metadata publish.", adapter.getUniqueName() );
        if ( publishers.containsKey( adapter.getUniqueName() ) ) return;
        MetadataListener listener = new AbstractListener();
        MetadataPublisher publisher = new AbstractPublisher<>( adapter, listener );
        publishers.put( adapter.getUniqueName(), publisher );
        publisher.start();
    }


    public void onAdapterUndeploy( String uniqueName ) {
        if ( publishers.containsKey( uniqueName ) ) {
            publishers.get( uniqueName ).stop();
            publishers.remove( uniqueName );
            log.error( "Adapter {} is going to be unregistered for metadata publish.", uniqueName );
        }
    }


}
