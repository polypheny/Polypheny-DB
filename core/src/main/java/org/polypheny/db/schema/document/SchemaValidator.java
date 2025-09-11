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
import java.util.Map;
import java.util.Set;

public final class SchemaValidator {

    private SchemaValidator() {}

    public static boolean conformsTo(DocumentSchema schema, BsonDocument doc) {
        // 1) required
        Set<String> required = schema.required();
        for (String key : required) {
            if (!doc.containsKey(key)) {
                return false;
            }
        }

        // 2) field types (only for those specified)
        for ( Map.Entry<String, DocumentSchema.FieldType> e : schema.types().entrySet()) {
            String key = e.getKey();
            DocumentSchema.FieldType expected = e.getValue();
            if (!doc.containsKey(key)) {
                // Not present → OK unless it's also in "required" (already checked)
                continue;
            }
            if (!matchesType(doc.get(key), expected)) {
                return false;
            }
        }

        // 3) additionalProperties
        if (schema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
            for (String k : doc.keySet()) {
                if (!required.contains(k) && !schema.types().containsKey(k)) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean matchesType( BsonValue v, DocumentSchema.FieldType t) {
        switch (t) {
            case BOOLEAN:
                return v instanceof BsonBoolean;
            case STRING:
                return v instanceof BsonString;
            case NUMBER:
                return (v instanceof BsonNumber) || v instanceof BsonDouble || v instanceof BsonInt32 || v instanceof BsonInt64;
            case INTEGER:
                return v instanceof BsonInt32 || v instanceof BsonInt64; // (accept both 32/64)
            case ARRAY:
                return v instanceof BsonArray;
            case OBJECT:
                return v instanceof BsonDocument;
            case DATE:
                // Mongo drivers represent dates as BsonDateTime (millis since epoch)
                return v instanceof BsonDateTime;
            case TIMESTAMP:
                return v instanceof BsonTimestamp;
            case BINARY:
                return v instanceof BsonBinary;
            default:
                return false;
        }
    }
}
