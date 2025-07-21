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

import java.util.Map;

/**
 * DocumentSchema is a lightweight holder for a document collection's schema definition.
 * It maps a collection ID to its set of defined fields (with constraints and types).
 *
 * This class is typically used in the context of validation or temporary schema inspection.
 */
public class DocumentSchema {

    // The unique ID of the document collection this schema belongs to
    public final long collectionId;

    // A mapping from field names to their definitions (type and constraints)
    public final Map<String, FieldDefinition> fields;

    /**
     * Constructs a new schema for a document collection.
     *
     * @param collectionId The ID of the collection this schema applies to
     * @param fields       Map of field names to their definitions
     */
    public DocumentSchema(long collectionId, Map<String, FieldDefinition> fields) {
        this.collectionId = collectionId;
        this.fields = fields;
    }
}
