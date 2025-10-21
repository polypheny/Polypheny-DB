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

import com.fasterxml.jackson.databind.ObjectMapper;

public final class SchemaJson {
    private static final ObjectMapper M = new ObjectMapper();
    private SchemaJson() {}

    public static DocumentSchema parse(String json) {
        try {
            // accept {"root": {...}} or bare {...}
            var node = M.readTree(json);
            if (node.isTextual()) node = M.readTree(node.asText());
            if (!node.has("root")) {
                var wrapper = M.createObjectNode();
                wrapper.set("root", node);
                node = wrapper;
            }
            return M.treeToValue(node, DocumentSchema.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DocumentSchema JSON: " + e.getMessage(), e);
        }
    }

    public static String toJson(DocumentSchema schema) {
        try {
            return M.writeValueAsString(schema); // canonical {"root": {...}}
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize DocumentSchema: " + e.getMessage(), e);
        }
    }
}
