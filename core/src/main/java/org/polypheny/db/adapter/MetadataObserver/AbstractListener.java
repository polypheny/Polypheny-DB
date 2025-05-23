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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;

@Slf4j
public class AbstractListener<P extends Adapter & MetadataProvider> implements MetadataListener<P> {

    private boolean available;
    private AbstractNode currentNode;
    private P adapter;


    public AbstractListener() {
        available = true;
        currentNode = null;
        this.adapter = null;
    }


    @Override
    public void onMetadataChange( P adapter, AbstractNode node, String hash ) {
        available ^= true;
        node = this.currentNode;
        this.adapter = adapter;
        log.info( "Listener saved credentials of adapter and sends now Request to UI and applies changes on adapter metadata and metadata the listener is holding." );
        applyChange();
    }


    @Override
    public void applyChange() {
        available ^= true;
        log.info( "Changes are going to be applied" );
    }


    @Override
    public boolean isAvailable() {
        return this.available;
    }

}

