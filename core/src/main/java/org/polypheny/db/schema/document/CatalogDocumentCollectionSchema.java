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

package org.polypheny.db.schema.document;

import io.activej.serializer.annotations.Serialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * CatalogDocumentCollectionSchema defines the persistent schema associated with a document collection.
 * It holds the collection ID and a map of fields, where each field includes type and constraint info.
 *
 * This class is used internally to store schema definitions in the catalog layer.
 */
public class CatalogDocumentCollectionSchema {

    // The unique ID of the document collection this schema belongs to
    @Serialize
    @JsonProperty
    public long collectionId;

    // A mapping of field names to their definitions (type, notNull, unique)
    @Serialize
    @JsonProperty
    public Map<String, FieldDefinition> fields;

    /**
     * Default constructor required for ActiveJ serialization.
     * Fields will be populated via deserialization.
     */
    public CatalogDocumentCollectionSchema() {
    }

    /**
     * Constructs a new schema definition for a specific collection.
     *
     * @param collectionId The ID of the document collection
     * @param fields       A map of field names to their FieldDefinition objects
     */
    public CatalogDocumentCollectionSchema(long collectionId, Map<String, FieldDefinition> fields) {
        this.collectionId = collectionId;
        this.fields = fields;
    }
}
