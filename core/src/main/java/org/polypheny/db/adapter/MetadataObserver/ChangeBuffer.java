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

import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.MetadataObserver.Utils.SimpleDiff;
import org.polypheny.db.adapter.MetadataObserver.Utils.SimpleDiffUtils;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import java.util.List;
import java.util.Optional;

public class ChangeBuffer<P extends Adapter & MetadataProvider> {

    private final P adapter;
    private final String adapterHash;

    private volatile boolean hasChanges = false;
    private List<SimpleDiff> diffs = List.of();


    public ChangeBuffer( final P adapter, final String adapterHash ) {
        this.adapter = adapter;
        this.adapterHash = adapterHash;
    }


    public synchronized void push( AbstractNode node ) {
        this.diffs = SimpleDiffUtils.findAddedNodes( adapter.getRoot(), node );
        this.hasChanges = !diffs.isEmpty();
    }


    public synchronized Optional<ChangeDTO> consume() {
        if ( !hasChanges ) {
            return Optional.empty();
        }

        ChangeDTO dto = new ChangeDTO(
                adapterHash,
                List.of( adapter.getUniqueName() ),
                diffs
        );
        hasChanges = false;
        diffs = List.of();
        return Optional.of( dto );
    }


    public record ChangeDTO(
            String adapterHash,
            List<String> credentials,
            List<SimpleDiff> diffs
    ) {

    }


}



