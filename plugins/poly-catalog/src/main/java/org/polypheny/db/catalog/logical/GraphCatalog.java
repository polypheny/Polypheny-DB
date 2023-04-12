/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.logical;

import io.activej.serializer.BinarySerializer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;

@Value
@SuperBuilder(toBuilder = true)
public class GraphCatalog implements Serializable, LogicalGraphCatalog {

    @Getter
    public BinarySerializer<GraphCatalog> serializer = Serializable.builder.get().build( GraphCatalog.class );
    @Getter
    public LogicalNamespace logicalNamespace;
    public IdBuilder idBuilder = IdBuilder.getInstance();

    @Getter
    ConcurrentHashMap<Long, LogicalGraph> graphs;


    @NonFinal
    @Builder.Default
    boolean openChanges = false;


    public GraphCatalog( LogicalNamespace logicalNamespace ) {
        this( logicalNamespace, new ConcurrentHashMap<>() );
    }


    public GraphCatalog( LogicalNamespace logicalNamespace, Map<Long, LogicalGraph> graphs ) {
        this.logicalNamespace = logicalNamespace;
        this.graphs = new ConcurrentHashMap<>( graphs );
    }


    @Override
    public GraphCatalog copy() {
        return deserialize( serialize(), GraphCatalog.class );
    }


    @Override
    public LogicalCatalog withLogicalNamespace( LogicalNamespace namespace ) {
        return toBuilder().logicalNamespace( namespace ).build();
    }


    @Override
    public void addGraphAlias( long graphId, String alias, boolean ifNotExists ) {

    }


    @Override
    public void removeGraphAlias( long graphId, String alias, boolean ifExists ) {

    }


    @Override
    public long addGraph( String name, List<DataStore> stores, boolean modifiable, boolean ifNotExists, boolean replace ) {
        return 0;
    }


    @Override
    public void deleteGraph( long id ) {

    }

}
