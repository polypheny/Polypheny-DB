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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeSupport;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.util.CatalogEvent;
import org.polypheny.db.schema.document.SchemaMeta;
import org.polypheny.db.type.PolySerializable;

@Value
@SuperBuilder(toBuilder = true)
public class DocumentCatalog implements PolySerializable, LogicalDocumentCatalog {

    public BinarySerializer<DocumentCatalog> serializer =
            PolySerializable.buildSerializer( DocumentCatalog.class );

    IdBuilder idBuilder = IdBuilder.getInstance();

    @Serialize
    @JsonProperty
    public LogicalNamespace logicalNamespace;
    @Serialize
    @JsonProperty
    public Map<Long, LogicalCollection> collections;

    @Serialize
    @JsonProperty
    public Map<Long, SchemaMeta> collectionSchemas;

    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public DocumentCatalog( LogicalNamespace logicalNamespace ) {
        this( logicalNamespace, Map.of(), Map.of() );
    }


    public DocumentCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("collections") Map<Long, LogicalCollection> collections,
            @Deserialize("collectionSchemas") Map<Long, SchemaMeta> collectionSchemas
    ) {
        this.logicalNamespace = logicalNamespace;
        this.collections = (collections == null)
                ? Map.of()
                : new ConcurrentHashMap<>( collections );
        this.collectionSchemas = (collectionSchemas == null)
                ? new ConcurrentHashMap<>()
                : new ConcurrentHashMap<>( collectionSchemas );

        listeners.addPropertyChangeListener( Catalog.getInstance().getChangeListener() );
    }


    public void change( CatalogEvent event, Object oldValue, Object newValue ) {
        listeners.firePropertyChange( event.name(), oldValue, newValue );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), DocumentCatalog.class );
    }


    @Override
    public LogicalCollection addCollection( String name, EntityType entity, boolean modifiable ) {
        long id = idBuilder.getNewLogicalId();
        LogicalCollection collection =
                new LogicalCollection( id, name, logicalNamespace.id, entity, modifiable );
        collections.put( id, collection );
        change( CatalogEvent.LOGICAL_DOC_ENTITY_CREATED, null, collection );
        return collection;
    }


    @Override
    public void deleteCollection( long id ) {
        collections.remove( id );
        collectionSchemas.remove( id );
        change( CatalogEvent.LOGICAL_DOC_ENTITY_DROPPED, id, null );
    }


    @Override
    public void renameCollection( LogicalCollection collection, String newName ) {
        LogicalCollection newCollection = collection.toBuilder().name( newName ).build();
        collections.put( newCollection.id, newCollection );
        change( CatalogEvent.LOGICAL_DOC_ENTITY_RENAMED, collection, newCollection );
    }


    @Override
    public LogicalCatalog withLogicalNamespace( LogicalNamespace namespace ) {
        return toBuilder().logicalNamespace( namespace ).build();
    }


    /**
     * Creates or updates the schema attached to an existing collection.
     */
    @Override
    public void upsertCollectionSchema( long collectionId, String schemaJson, String enforcement ) {
        SchemaMeta old = collectionSchemas.get( collectionId );
        long nextVersion = (old == null) ? 1 : (old.version + 1);
        SchemaMeta meta = new SchemaMeta(
                schemaJson,
                enforcement,
                nextVersion
        );
        collectionSchemas.put( collectionId, meta );
    }


    /**
     * Drop schema metadata for a collection.
     */
    @Override
    public void dropCollectionSchema( long collectionId ) {
        collectionSchemas.remove( collectionId );
    }


    /**
     * Lookup schema metadata for a collection.
     */
    @Override
    public Optional<SchemaMeta> getCollectionSchema( long collectionId ) {
        return Optional.ofNullable( collectionSchemas.get( collectionId ) );
    }

}
