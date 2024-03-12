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

package org.polypheny.db.catalog.impl.logical;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeSupport;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.util.CatalogEvent;
import org.polypheny.db.type.PolySerializable;

@Value
@SuperBuilder(toBuilder = true)
public class GraphCatalog implements PolySerializable, LogicalGraphCatalog {

    @Getter
    public BinarySerializer<GraphCatalog> serializer = PolySerializable.buildSerializer( GraphCatalog.class );
    @Getter
    @Serialize
    public LogicalNamespace logicalNamespace;
    public IdBuilder idBuilder = IdBuilder.getInstance();

    @Getter
    @Serialize
    public ConcurrentHashMap<Long, LogicalGraph> graphs;


    public GraphCatalog( LogicalNamespace logicalNamespace ) {
        this( logicalNamespace, new ConcurrentHashMap<>() );
    }


    public GraphCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("graphs") Map<Long, LogicalGraph> graphs ) {
        this.logicalNamespace = logicalNamespace;
        this.graphs = new ConcurrentHashMap<>( graphs );

        // already add only graph
        addGraph( logicalNamespace.id, logicalNamespace.name, true );
        listeners.addPropertyChangeListener( Catalog.getInstance().getChangeListener() );
    }


    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public void change( CatalogEvent event, Object oldValue, Object newValue ) {
        listeners.firePropertyChange( event.name(), oldValue, newValue );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), GraphCatalog.class );
    }


    @Override
    public LogicalCatalog withLogicalNamespace( LogicalNamespace namespace ) {
        return toBuilder().logicalNamespace( namespace ).build();
    }


    @Override
    public void addGraphAlias( long graphId, String alias, boolean ifNotExists ) {
        change( CatalogEvent.LOGICAL_GRAPH_ENTITY_RENAMED, null, alias );
    }


    @Override
    public void removeGraphAlias( long graphId, String alias, boolean ifExists ) {
        change( CatalogEvent.LOGICAL_GRAPH_ENTITY_RENAMED, alias, null );
    }


    @Override
    public LogicalGraph addGraph( long id, String name, boolean modifiable ) {
        LogicalGraph graph = new LogicalGraph( id, name, modifiable, logicalNamespace.caseSensitive );
        graphs.put( id, graph );
        change( CatalogEvent.LOGICAL_GRAPH_ENTITY_CREATED, null, graph );
        return graph;
    }


    @Override
    public void deleteGraph( long id ) {
        graphs.remove( id );
        change( CatalogEvent.LOGICAL_GRAPH_ENTITY_DROPPED, id, null );
    }

}
