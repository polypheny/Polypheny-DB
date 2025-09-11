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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.polypheny.db.schema.document.DocumentSchema.FieldType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class SchemaOptionsResolver {

    public static final class Resolved {
        public final DocumentSchema schema;      // nullable
        public final EnforcementMode mode;       // never null (default OFF/STRICT as you prefer)
        public Resolved(DocumentSchema s, EnforcementMode m) { this.schema = s; this.mode = m; }
    }

    private static final ObjectMapper M = new ObjectMapper();

    /**
     * Accepts a language-specific JSON string (e.g. MQL options), and tries to extract a schema.
     * Supports either:
     *   - Mongo-style: {"validator":{"$jsonSchema":{...}}, "validationAction":"error|warn"}
     *   - shorthand:   {"validator":{"required":[...], "types":{...}, "additionalProperties":"FORBID"}, "validationAction":"STRICT|WARN|OFF"}
     * Returns null schema if nothing is provided.
     */
    public static Resolved resolve(@SuppressWarnings("NullableProblems") String optionsJson) {
        if (optionsJson == null || optionsJson.isBlank()) {
            return new Resolved(null, EnforcementMode.OFF);
        }

        try {
            JsonNode root = M.readTree(optionsJson);

            // default mode
            EnforcementMode mode = EnforcementMode.OFF;
            if (root.has("validationAction")) {
                String s = root.get("validationAction").asText("");
                mode = mapMode(s);
            }

            if (!root.has("validator")) {
                return new Resolved(null, mode);
            }

            JsonNode validator = root.get("validator");
            DocumentSchema schema = null;

            // Case A: $jsonSchema (Mongo)
            if (validator.has("$jsonSchema")) {
                // re-use your existing mapper:
                schema = MongoJsonSchemaMapper.toDocumentSchema(validator.get("$jsonSchema"));
            }
            // Case B: shorthand (required/types/additionalProperties)
            else if (validator.has("required") || validator.has("types") || validator.has("additionalProperties")) {
                schema = ShorthandSchemaMapper.toDocumentSchema(validator);
            }

            return new Resolved(schema, mode);
        } catch (Exception e) {
            // treat as "no schema" instead of crashing DDL; or rethrow if you want strict behavior
            return new Resolved(null, EnforcementMode.OFF);
        }
    }

    private static EnforcementMode mapMode(String s) {
        String x = s == null ? "" : s.toLowerCase( Locale.ROOT);
        return switch (x) {
            case "error", "strict" -> EnforcementMode.STRICT;
            case "warn"            -> EnforcementMode.WARN;
            default                -> EnforcementMode.OFF;
        };
    }

    // --- minimal mappers you already sketched (put in same package) ---
    static final class MongoJsonSchemaMapper {
        static DocumentSchema toDocumentSchema(JsonNode js) {
            Set<String> req = new HashSet<>();
            if (js.has("required") && js.get("required").isArray()) {
                js.get("required").forEach(n -> req.add(n.asText()));
            }
            Map<String, FieldType> types = new HashMap<>();
            if (js.has("properties") && js.get("properties").isObject()) {
                js.get("properties").fields().forEachRemaining(e -> {
                    types.put(e.getKey(), mapBsonType(e.getValue()));
                });
            }
            DocumentSchema.AdditionalProperties ap = DocumentSchema.AdditionalProperties.ALLOW;
            if (js.has("additionalProperties") && js.get("additionalProperties").isBoolean()) {
                ap = js.get("additionalProperties").asBoolean()
                        ? DocumentSchema.AdditionalProperties.ALLOW
                        : DocumentSchema.AdditionalProperties.FORBID;
            }
            return new DocumentSchema(req, types, ap);
        }

        private static DocumentSchema.FieldType mapBsonType(JsonNode prop) {
            String t = "object";
            if (prop.has("bsonType")) {
                JsonNode bt = prop.get("bsonType");
                t = bt.isArray() ? bt.get(0).asText() : bt.asText();
            }
            return switch (t.toLowerCase(Locale.ROOT)) {
                case "bool", "boolean" -> DocumentSchema.FieldType.BOOLEAN;
                case "string"          -> DocumentSchema.FieldType.STRING;
                case "double", "number"-> DocumentSchema.FieldType.NUMBER;
                case "int", "long", "int32", "int64", "decimal", "decimal128" -> DocumentSchema.FieldType.INTEGER;
                case "array"           -> DocumentSchema.FieldType.ARRAY;
                case "object"          -> DocumentSchema.FieldType.OBJECT;
                case "date"            -> DocumentSchema.FieldType.DATE;
                case "timestamp"       -> DocumentSchema.FieldType.TIMESTAMP;
                case "bindata", "binary" -> DocumentSchema.FieldType.BINARY;
                default -> DocumentSchema.FieldType.OBJECT;
            };
        }
    }

    static final class ShorthandSchemaMapper {
        static DocumentSchema toDocumentSchema(JsonNode v) {
            Set<String> req = new HashSet<>();
            if (v.has("required")) v.get("required").forEach(n -> req.add(n.asText()));

            Map<String, DocumentSchema.FieldType> types = new HashMap<>();
            if (v.has("types")) {
                v.get("types").fields().forEachRemaining(e ->
                        types.put(e.getKey(), DocumentSchema.FieldType.valueOf(e.getValue().asText().toUpperCase(Locale.ROOT)))
                );
            }

            DocumentSchema.AdditionalProperties ap = DocumentSchema.AdditionalProperties.ALLOW;
            if (v.has("additionalProperties")) {
                JsonNode apNode = v.get("additionalProperties");
                if (apNode.isBoolean()) {
                    ap = apNode.asBoolean() ? DocumentSchema.AdditionalProperties.ALLOW : DocumentSchema.AdditionalProperties.FORBID;
                } else {
                    ap = "FORBID".equalsIgnoreCase(apNode.asText())
                            ? DocumentSchema.AdditionalProperties.FORBID
                            : DocumentSchema.AdditionalProperties.ALLOW;
                }
            }
            return new DocumentSchema(req, types, ap);
        }
    }
}
