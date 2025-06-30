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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.MetadataObserver.ChangeLogEntry;
import org.polypheny.db.adapter.MetadataObserver.ChangeLogEntry.DiffMessageUtil;
import org.polypheny.db.adapter.MetadataObserver.PublisherManager.ChangeStatus;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaAnnotator;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil.DiffResult;
import org.polypheny.db.adapter.java.AdapterTemplate.PreviewResult;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.NodeSerializer;
import org.polypheny.db.schemaDiscovery.NodeUtil;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Slf4j
public class AbstractListener<P extends Adapter & MetadataProvider> implements MetadataListener<P> {

    private boolean available;
    private AbstractNode currentNode;
    private P adapter;
    private String hash;

    private static final Gson GSON = new Gson();


    public AbstractListener() {
        available = true;
        currentNode = null;
        this.adapter = null;
        this.hash = null;
    }


    @Override
    public void onMetadataChange( P adapter, AbstractNode node, String hash ) {
        available ^= true;
        this.currentNode = node;
        this.adapter = adapter;
        this.hash = hash;

        Object preview = adapter.getPreview();

        log.info( "Listener saved credentials of adapter and sends now Request to UI and applies changes on adapter metadata and metadata the listener is holding." );

        DiffResult result = MetaDiffUtil.diff( adapter.getRoot(), node );
        log.info( "Diffresult: {}", result );

        ChangeStatus status = NodeUtil.evaluateStatus( result, adapter.getRoot() );

        ChangeLogEntry entry = new ChangeLogEntry( adapter.getUniqueName(), Instant.now().toString(), DiffMessageUtil.toMessages( result ), status );
        PublisherManager.getInstance().addChange( entry );

        AbstractNode annotatedCopy = MetaAnnotator.annotateTree( adapter.getRoot(), node, result );
        String json = NodeSerializer.serializeNode( annotatedCopy ).toString();
        log.info( "JSON: {}", json );

        PublisherManager.getInstance().onMetadataChange( adapter.getUniqueName(), new PreviewResult( json, preview, List.of( entry ) ), status );
    }


    @Override
    public void applyChange( String[] metadata ) {
        log.info( "Changes are going to be applied" );

        Set<String> prevSelected = NodeUtil.collectSelecedAttributePaths( this.adapter.getRoot() );

        this.adapter.setRoot( this.currentNode );
        if ( metadata != null && metadata.length > 0 ) {
            prevSelected.addAll( Arrays.asList( metadata ) );
        }
        this.adapter.markSelectedAttributes( List.copyOf( prevSelected ) );
        HashCache.getInstance().put( this.adapter.getUniqueName(), this.hash );

        this.currentNode = null;
        this.adapter = null;
        this.hash = null;

        available ^= true;

    }


    @Override
    public boolean isAvailable() {
        return this.available;
    }

}

