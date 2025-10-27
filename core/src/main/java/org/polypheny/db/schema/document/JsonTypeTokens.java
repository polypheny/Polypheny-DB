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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.polypheny.db.type.PolyType;
import java.util.Locale;

public final class JsonTypeTokens {
    private JsonTypeTokens() {}

    // --- Legacy normalization (READ path) ---
    private static String normalizeToJsonToken(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        int paren = s.indexOf('('); // tolerate legacy e.g. VARCHAR(50)
        if (paren >= 0) s = s.substring(0, paren);
        String t = s.toLowerCase(Locale.ROOT);

        // Strict JSON tokens pass through
        switch (t) {
            case "string": case "text":
            case "number":
            case "boolean": case "bool":
            case "null":
            case "object": case "array":
                return t;
        }
        // Minimal legacy compatibility for already-persisted schemas
        switch (t) {
            case "double":           return "number";
            case "varchar":
            case "char":
            case "json":
            case "text":             return "string";
            case "boolean":          return "boolean";
            case "null":             return "null";
            default:                 return t; // anything else should still error
        }
    }

    // Map user/stored token -> PolyType (READ)
    public static PolyType toPolyType(String raw) {
        if (raw == null) throw new IllegalArgumentException("Type token must be provided");
        String t = normalizeToJsonToken(raw);
        switch (t) {
            case "string": case "text":
                return PolyType.TEXT;      // JSON string
            case "number":
                return PolyType.DOUBLE;    // JSON number (validator accepts all BSON numeric encodings)
            case "boolean": case "bool":
                return PolyType.BOOLEAN;
            case "null":
                return PolyType.NULL;
            case "object": case "array":
                throw new IllegalArgumentException(
                        "Structural types require nested specification: object/array with properties/items");
            default:
                throw new IllegalArgumentException(
                        "Unsupported type '" + raw + "'. Allowed: string (text), number, boolean, null, object, array.");
        }
    }

    // Canonical JSON token for a PolyType (WRITE)
    public static String toJsonToken(PolyType t) {
        return switch (t) {
            case TEXT, CHAR, VARCHAR -> "string";
            case DOUBLE             -> "number";
            case BOOLEAN            -> "boolean";
            case NULL               -> "null";
            default                 -> throw new IllegalArgumentException("Cannot serialize non-JSON PolyType: " + t);
        };
    }

    // Shared scalar check used by validator/enforcer
    public static boolean isBsonNumeric(BsonValue v) {
        return v instanceof BsonNumber
                || (v != null && v.getBsonType() == BsonType.DECIMAL128);
    }

    public static boolean matchesJson(BsonValue v, PolyType t) {
        if (t == PolyType.NULL) return v == null || v.isNull();
        return switch (t) {
            case BOOLEAN -> v instanceof BsonBoolean;
            case CHAR, VARCHAR, TEXT -> v instanceof BsonString; // JSON string
            case DOUBLE -> isBsonNumeric(v);                     // JSON number
            case ARRAY -> v instanceof BsonArray;                // defensive
            case MAP   -> v instanceof BsonDocument;             // defensive
            default    -> false;
        };
    }

    public static boolean isJsonNumberPolyType(PolyType t) {
        return t == PolyType.DOUBLE;
    }
}
