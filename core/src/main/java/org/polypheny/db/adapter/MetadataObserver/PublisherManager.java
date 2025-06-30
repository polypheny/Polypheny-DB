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

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil.DiffResult;
import org.polypheny.db.adapter.java.AdapterTemplate.PreviewResult;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class PublisherManager {

    private static final int MAX_ENTRIES_PER_ADAPTER = 100;

    private final Map<String, MetadataPublisher> publishers = new ConcurrentHashMap<>();
    private final Map<String, PreviewResult> changeCache = new ConcurrentHashMap<>();
    private final Map<String, ChangeStatus> statusCache = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Deque<ChangeLogEntry>> changeCatalog = new ConcurrentHashMap<>();

    private static final PublisherManager INSTANCE = new PublisherManager();


    public static PublisherManager getInstance() {
        return INSTANCE;
    }


    private PublisherManager() {
    }


    public <P extends Adapter & MetadataProvider> void onAdapterDeploy( P adapter ) {
        log.info( "Adapter {} is going to be registered for metadata publish.", adapter.getUniqueName() );
        if ( publishers.containsKey( adapter.getUniqueName() ) ) {
            return;
        }
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


    public ChangeStatus hasChange( String uniqueName ) {
        if ( changeCache.containsKey( uniqueName ) ) {
            return statusCache.get( uniqueName );
        } else {
            return null;
        }
    }


    public void onMetadataChange( String uniqueName, PreviewResult data, ChangeStatus status ) {
        changeCache.put( uniqueName, data );
        statusCache.put( uniqueName, status );
    }


    public PreviewResult fetchChange( String uniqueName ) {
        return changeCache.get( uniqueName );
    }


    public void ack( String uniqueName, String[] metadata ) {
        MetadataPublisher publisher = publishers.get( uniqueName );
        publisher.getListener().applyChange( metadata );
        changeCache.remove( uniqueName );
        statusCache.remove( uniqueName );
    }


    public enum ChangeStatus {
        CRITICAL,
        WARNING,
        OK
    }


    public void addChange( ChangeLogEntry entry ) {
        changeCatalog.computeIfAbsent( entry.getAdapterName(), k -> new ConcurrentLinkedDeque<>() ).addFirst( entry );
    }


    public List<ChangeLogEntry> getHistory( String adapterName ) {
        return changeCatalog.getOrDefault( adapterName, new ConcurrentLinkedDeque<>() )
                .stream()
                .toList();
    }


    private void prune( String adapterName ) {
        Deque<ChangeLogEntry> deque = changeCatalog.get( adapterName );
        while ( deque != null && deque.size() > MAX_ENTRIES_PER_ADAPTER ) {
            deque.removeLast();
        }
    }

}
