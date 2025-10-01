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

import com.mongodb.lang.Nullable;
import java.util.Map;

public final class SchemaCompatibility {

    private SchemaCompatibility() {}

    /**
     * Returns true if applying {@code proposed} on top of {@code current} is guaranteed
     * not to require scanning/changing existing data.
     */
    public static boolean isCompatible(@Nullable DocumentSchema current, DocumentSchema proposed) {
        if (proposed == null) {
            return true;
        }
        if (current == null) {
            // Without scanning, we only accept a "no-op" schema: no required, no types, ALLOW extras.
            return proposed.required().isEmpty()
                    && proposed.types().isEmpty()
                    && proposed.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW;
        }

        // 1) required may only shrink
        if (!current.required().containsAll(proposed.required())) {
            return false; // new schema adds required fields
        }

        // 2) types: proposed may drop constraints (subset of keys), or widen INTEGER -> NUMBER
        for ( Map.Entry<String, DocumentSchema.FieldType> e : proposed.types().entrySet()) {
            String field = e.getKey();
            DocumentSchema.FieldType newType = e.getValue();
            DocumentSchema.FieldType oldType = current.types().get(field);
            if (oldType == null) {
                // Adding a type constraint where previously none existed -> tightening
                return false;
            }
            if (!isWideningOrEqual(oldType, newType)) {
                return false;
            }
        }
        // removing old constraints is okay

        // 3) additionalProperties: ALLOW -> FORBID is tightening (disallowed), others okay
        if (current.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW
                && proposed.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
            return false;
        }

        return true;
    }

    private static boolean isWideningOrEqual(DocumentSchema.FieldType oldT, DocumentSchema.FieldType newT) {
        if (oldT == newT) return true;
        // widening: integer ⊂ number
        return oldT == DocumentSchema.FieldType.INTEGER && newT == DocumentSchema.FieldType.NUMBER;
    }
}
