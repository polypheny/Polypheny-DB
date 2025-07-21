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

/**
 * FieldDefinition represents a single field inside a document collection schema.
 * Each field has a name, a type, and optional constraints (not null, unique).
 * This class is serializable using both ActiveJ (for internal persistence) and Jackson (for JSON).
 */
public class FieldDefinition {

    // The name of the field (e.g., "title", "year")
    @Serialize
    @JsonProperty
    public String name;

    // The data type of the field as a string (e.g., "STRING", "INT", "BOOLEAN")
    @Serialize
    @JsonProperty
    public String type;

    // If true, the field must not be null in any document
    @Serialize
    @JsonProperty
    public boolean notNull;

    // If true, this field must be unique across all documents in the collection
    @Serialize
    @JsonProperty
    public boolean unique;

    /**
     * No-argument constructor required by ActiveJ for deserialization.
     * Fields will be set manually by the deserializer.
     */
    public FieldDefinition() {
    }

    /**
     * Full constructor to create a FieldDefinition with all properties specified.
     *
     * @param name    Name of the field
     * @param type    Data type of the field
     * @param notNull Whether the field is required (NOT NULL constraint)
     * @param unique  Whether the field must be unique across documents
     */
    public FieldDefinition(String name, String type, boolean notNull, boolean unique) {
        this.name = name;
        this.type = type;
        this.notNull = notNull;
        this.unique = unique;
    }
}
