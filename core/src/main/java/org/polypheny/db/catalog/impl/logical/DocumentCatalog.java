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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.schema.document.FieldDefinition;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.util.CatalogEvent;
import org.polypheny.db.schema.document.CatalogDocumentCollectionSchema;
import org.polypheny.db.type.PolySerializable;

@Value
@SuperBuilder(toBuilder = true)
public class DocumentCatalog implements PolySerializable, LogicalDocumentCatalog {

    public BinarySerializer<DocumentCatalog> serializer = PolySerializable.buildSerializer( DocumentCatalog.class );

    IdBuilder idBuilder = IdBuilder.getInstance();

    @Serialize
    @JsonProperty
    public LogicalNamespace logicalNamespace;

    @Serialize
    @JsonProperty
    public Map<Long, LogicalCollection> collections;

    @Serialize
    @JsonProperty
    public Map<Long, CatalogDocumentCollectionSchema> collectionSchemas = new ConcurrentHashMap<>();

    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public DocumentCatalog( LogicalNamespace logicalNamespace ) {
        this( logicalNamespace, Map.of() );
    }


    public DocumentCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("collections") Map<Long, LogicalCollection> collections ) {
        this.logicalNamespace = logicalNamespace;
        this.collections = new ConcurrentHashMap<>( collections );

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
        LogicalCollection collection = new LogicalCollection( id, name, logicalNamespace.id, entity, modifiable );
        collections.put( id, collection );
        change( CatalogEvent.LOGICAL_DOC_ENTITY_CREATED, null, collection );
        return collection;
    }


    @Override
    public void deleteCollection( long id ) {
        collections.remove( id );
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

    // Overloaded method to create a document collection along with an associated schema.
    public LogicalCollection addCollection(String name, EntityType entity, boolean modifiable, CatalogDocumentCollectionSchema schema) {
        // Delegate to existing method to create the collection
        LogicalCollection collection = addCollection(name, entity, modifiable);

        // Store the schema for the newly created collection using its ID
        collectionSchemas.put(collection.id, schema);

        return collection;
    }

    // Validates a document against the schema defined for the specified collection.
    // If no schema is associated (schemaless), the document is accepted as-is.
    public void validateDocument(long collectionId, Map<String, Object> document) {
        CatalogDocumentCollectionSchema schema = collectionSchemas.get(collectionId);

        // If the collection is schemaless, no validation is performed
        if (schema == null) return;

        for (FieldDefinition field : schema.fields.values()) {
            Object value = document.get(field.name);

            // Check not-null constraint
            if (field.notNull && value == null) {
                throw new IllegalArgumentException("Field '" + field.name + "' cannot be null.");
            }

            // Check type if value is present
            if (value != null && !validateType(field.type, value)) {
                throw new IllegalArgumentException("Field '" + field.name + "' has incorrect type.");
            }
        }
    }

    // Internal helper method to verify that a value matches the expected field type.
    // Returns true if the value is of the correct Java type based on declared schema type.
    private boolean validateType(String type, Object value) {
        return switch (type.toUpperCase()) {
            case "STRING" -> value instanceof String;
            case "INT" -> value instanceof Integer;
            case "BOOLEAN" -> value instanceof Boolean;
            default -> false; // Unrecognized types fail validation
        };
    }

    // Updates or replaces the schema for an existing collection.
    // Emits a change event to inform listeners (optional use of event type).
    public void alterSchema(long collectionId, CatalogDocumentCollectionSchema newSchema) {
        collectionSchemas.put(collectionId, newSchema);

        // Emits an event to indicate that the schema was added or modified
        change(CatalogEvent.LOGICAL_DOC_ENTITY_CREATED, null, newSchema); // Consider using LOGICAL_DOC_ENTITY_MODIFIED
    }

    // Removes the schema associated with a given collection ID.
        // This effectively makes the collection schemaless again.
    public void dropSchema(long collectionId) {
        collectionSchemas.remove(collectionId);

        // Emits a change event to notify listeners (optional usage)
        change(CatalogEvent.LOGICAL_DOC_ENTITY_CREATED, null, null); // Consider using LOGICAL_DOC_ENTITY_MODIFIED
    }


}
