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
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.NodeSerializer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class AbstractPublisher<P extends Adapter & MetadataProvider> implements MetadataPublisher {

    protected final P provider;
    private final long intervalSeconds = 30;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private MetadataListener listener;
    private final MetadataHasher hasher = new MetadataHasher();
    private final HashCache cache = HashCache.getInstance();


    protected AbstractPublisher( P provider, MetadataListener listener ) {
        this.provider = provider;
        this.listener = listener;
    }


    @Override
    public String getAdapterUniqueName() {
        return provider.getUniqueName();
    }


    @Override
    public void start() {
        scheduler.scheduleAtFixedRate( this::runCheck, 0, intervalSeconds, java.util.concurrent.TimeUnit.SECONDS );
    }


    @Override
    public void stop() {
        scheduler.shutdown();
    }


    @Override
    public void runCheck() {
        if ( !listener.isAvailable() ) return;
        try {
            AbstractNode node = provider.fetchMetadataTree();
            String fresh = NodeSerializer.serializeNode( node ).toString();

            String hash = hasher.hash( fresh );
            String lastHash = cache.getHash( provider.getUniqueName() );

            log.info("Fresh JSON: {}", fresh);
            log.info( "Metadata hash at Observer-Check (Current adapter hash) : {}", lastHash );
            log.info( "Metadata hash at Observer-Check (Newest hash) : {}", hash );
            log.info("Key used during observer-check: {}", provider.getUniqueName());



            if ( lastHash != null && !lastHash.equals( hash ) ) {
                log.info( "Metadata of adapter {} changed. Sending new snapshot to UI.", provider.getUniqueName() );
                listener.onMetadataChange( provider, node, hash );
            } else {
                log.info( "Metadata of adapter {} did not change.", provider.getUniqueName() );
            }
        } catch ( Exception e ) {
            throw new RuntimeException( "Error while checking current snapshot.", e );
        }
    }

}
