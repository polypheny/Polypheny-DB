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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class SchemaValidator {

    private SchemaValidator() {}

    // === NEW: rich validation result ===
    public record Violation(String path, String code, String message) {}
    public record ValidationResult(boolean ok, List<Violation> violations) {
        public String compactSummary(int maxItems) {
            if (ok || violations.isEmpty()) return "ok";
            return violations.stream()
                    .limit(Math.max(1, maxItems))
                    .map(v -> v.code + "@" + v.path + "(" + v.message + ")")
                    .collect( Collectors.joining("; "))
                    + (violations.size() > maxItems ? " … +" + (violations.size() - maxItems) + " more" : "");
        }
    }

    /** NEW: use this for WARN logs + STRICT errors. */
    public static ValidationResult validate(DocumentSchema schema, BsonDocument doc) {
        List<Violation> out = new ArrayList<>();

        // 1) required
        Set<String> required = schema.required();
        for (String key : required) {
            if (!doc.containsKey(key) || doc.get(key).isNull()) {
                out.add(new Violation(key, "REQUIRED_MISSING", "Required field is missing"));
            }
        }

        // 2) field types (only for those specified)
        for (Map.Entry<String, DocumentSchema.FieldType> e : schema.types().entrySet()) {
            String key = e.getKey();
            if (!doc.containsKey(key) || doc.get(key).isNull()) {
                continue; // required already handled
            }
            if (!matchesType(doc.get(key), e.getValue())) {
                out.add(new Violation(key, "TYPE_MISMATCH",
                        "Expected " + e.getValue() + " but got " + bsonTypeName(doc.get(key))));
            }
        }

        // 3) additionalProperties
        if (schema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
            for (String k : doc.keySet()) {
                if (!required.contains(k) && !schema.types().containsKey(k)) {
                    out.add(new Violation(k, "ADDITIONAL_PROPERTY",
                            "Unexpected field while additionalProperties=FORBID"));
                }
            }
        }

        return new ValidationResult(out.isEmpty(), out);
    }

    /** Backward-compatible: still available for quick boolean checks. */
    public static boolean conformsTo(DocumentSchema schema, BsonDocument doc) {
        return validate(schema, doc).ok();
    }

    private static boolean matchesType(BsonValue v, DocumentSchema.FieldType t) {
        return switch (t) {
            case BOOLEAN   -> v instanceof BsonBoolean;
            case STRING    -> v instanceof BsonString;
            case NUMBER    -> v instanceof BsonNumber || v instanceof BsonDouble || v instanceof BsonInt32 || v instanceof BsonInt64;
            case INTEGER   -> v instanceof BsonInt32 || v instanceof BsonInt64;
            case ARRAY     -> v instanceof BsonArray;
            case OBJECT    -> v instanceof BsonDocument;
            case DATE      -> v instanceof BsonDateTime;   // Mongo dates as millis
            case TIMESTAMP -> v instanceof BsonTimestamp;
            case BINARY    -> v instanceof BsonBinary;
        };
    }

    private static String bsonTypeName(BsonValue v) {
        return v == null ? "NULL" : v.getBsonType().name();
    }
}
