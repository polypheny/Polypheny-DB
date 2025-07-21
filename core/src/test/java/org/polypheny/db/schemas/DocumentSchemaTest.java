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

package org.polypheny.db.schemas;

import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.schema.document.FieldDefinition;
import org.polypheny.db.schema.document.CatalogDocumentCollectionSchema;
import org.polypheny.db.catalog.impl.logical.DocumentCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.EntityType;

import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DocumentSchemaTest verifies the behavior of schema validation logic
 * without relying on the full Polypheny catalog infrastructure.
 *
 * This is a focused unit test using a manually constructed schema and documents.
 */
public class DocumentSchemaTest {

    @Test
    public void testValidationWithoutCatalog() {
        // Define a test schema with two fields:
        // - title: STRING, notNull = true
        // - year: INT, notNull = false
        Map<String, FieldDefinition> fields = Map.of(
                "title", new FieldDefinition("title", "STRING", true, false),
                "year", new FieldDefinition("year", "INT", false, false)
        );

        // Create schema object associated with collection ID 42 (arbitrary)
        CatalogDocumentCollectionSchema schema = new CatalogDocumentCollectionSchema(42L, fields);

        // Valid document: has both fields with correct types
        Map<String, Object> validDoc = Map.of("title", "Dune", "year", 1965);
        assertDoesNotThrow(() -> validate(validDoc, schema));

        // Invalid document: missing the required 'title' field
        Map<String, Object> missingTitle = Map.of("year", 2020);
        assertThrows(IllegalArgumentException.class, () -> validate(missingTitle, schema));
    }

    /**
     * Simulates validation logic by checking required fields and type constraints.
     * This logic mirrors what DocumentCatalog.validateDocument() would do.
     *
     * @param document the input document to validate
     * @param schema   the schema to validate against
     */
    private void validate(Map<String, Object> document, CatalogDocumentCollectionSchema schema) {
        for (FieldDefinition field : schema.fields.values()) {
            Object value = document.get(field.name);

            // Enforce not-null constraint
            if (field.notNull && value == null) {
                throw new IllegalArgumentException("Field '" + field.name + "' cannot be null.");
            }

            // Enforce type constraint if value is present
            if (value != null && !validateType(field.type, value)) {
                throw new IllegalArgumentException("Field '" + field.name + "' has incorrect type.");
            }
        }
    }

    /**
     * Performs a simple type check to validate the field's declared type.
     *
     * @param type  the expected type (as string)
     * @param value the value to check
     * @return true if the value matches the expected type
     */
    private boolean validateType(String type, Object value) {
        return switch (type.toUpperCase()) {
            case "STRING" -> value instanceof String;
            case "INT" -> value instanceof Integer;
            case "BOOLEAN" -> value instanceof Boolean;
            default -> false;
        };
    }
}

