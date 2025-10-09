/*
 * Copyright 2019-2025 The Polypheny Project
 * Licensed under the Apache License, Version 2.0
 */
package org.polypheny.db.schema.document;

import com.mongodb.lang.Nullable;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.type.PolyType;

public final class SchemaCompatibility {

    private SchemaCompatibility() {}

    /**
     * Returns true if applying {@code proposed} on top of {@code current} is
     * guaranteed NOT to require scanning/changing existing data.
     *
     * Rules (no-scan safe):
     * - You may NOT add new properties (all declared props are required).
     * - You may remove properties ONLY if the destination object's additionalProperties == ALLOW.
     * - You may not tighten additionalProperties (ALLOW -> FORBID).
     * - Scalars: only widening allowed (int -> numeric). Equal types are OK.
     * - Arrays: items must be compatible; minItems must NOT increase; uniqueItems must NOT turn from false->true.
     * - Object/Array/Scalar node kinds must not change.
     */
    public static boolean isCompatible(@Nullable DocumentSchema current, @Nullable DocumentSchema proposed) {
        if (proposed == null) return true; // nothing to apply
        if (current == null) {
            // Only accept a "no-op" schema: empty root with ALLOW extras
            return isNoopRoot(proposed);
        }
        return isObjectCompatible(current.root(), proposed.root());
    }

    // ---------- Object ----------

    private static boolean isObjectCompatible(DocumentSchema.ObjectNode cur, DocumentSchema.ObjectNode prop) {
        // additionalProperties: ALLOW -> FORBID is tightening (unsafe)
        if (cur.additionalProperties == DocumentSchema.AdditionalProperties.ALLOW
                && prop.additionalProperties == DocumentSchema.AdditionalProperties.FORBID) {
            return false;
        }

        // Adding properties? (proposed has a key that current does not) -> not allowed (would add new required field)
        for (Map.Entry<String, DocumentSchema.Node> e : prop.properties.entrySet()) {
            String k = e.getKey();
            DocumentSchema.Node curChild = cur.properties.get(k);
            if (curChild == null) {
                return false; // adding a required field
            }
            if (!isNodeCompatible(curChild, e.getValue())) {
                return false;
            }
        }

        // Removing properties? allowed only if proposed allows extras at this level
        if (prop.additionalProperties == DocumentSchema.AdditionalProperties.FORBID) {
            // if FORBID, every current property must still be declared in proposed
            for (String k : cur.properties.keySet()) {
                if (!prop.properties.containsKey(k)) {
                    return false;
                }
            }
        }
        // If ALLOW, old undeclared keys become "extras" but are permitted -> safe.

        return true;
    }

    // ---------- Node dispatcher ----------

    private static boolean isNodeCompatible(DocumentSchema.Node cur, DocumentSchema.Node prop) {
        if (cur instanceof DocumentSchema.ScalarNode && prop instanceof DocumentSchema.ScalarNode) {
            return isScalarWideningOrEqual(((DocumentSchema.ScalarNode) cur).type,
                    ((DocumentSchema.ScalarNode) prop).type);
        }
        if (cur instanceof DocumentSchema.ObjectNode && prop instanceof DocumentSchema.ObjectNode) {
            return isObjectCompatible((DocumentSchema.ObjectNode) cur, (DocumentSchema.ObjectNode) prop);
        }
        if (cur instanceof DocumentSchema.ArrayNode && prop instanceof DocumentSchema.ArrayNode) {
            return isArrayCompatible((DocumentSchema.ArrayNode) cur, (DocumentSchema.ArrayNode) prop);
        }
        // Changing node kind (scalar<->object/array, object<->array) is unsafe
        return false;
    }

    // ---------- Array ----------

    private static boolean isArrayCompatible(DocumentSchema.ArrayNode cur, DocumentSchema.ArrayNode prop) {
        // items schema must be compatible
        if (!isNodeCompatible(cur.items, prop.items)) return false;

        // minItems must NOT increase (tightening)
        int curMin = cur.minItems == null ? 0 : cur.minItems;
        int propMin = prop.minItems == null ? 0 : prop.minItems;
        if (propMin > curMin) return false;

        // uniqueItems must NOT turn from false/null -> true (tightening)
        boolean curUnique = Boolean.TRUE.equals(cur.uniqueItems);
        boolean propUnique = Boolean.TRUE.equals(prop.uniqueItems);
        if (!curUnique && propUnique) return false;

        return true;
    }

    // ---------- Scalars ----------

    private static boolean isScalarWideningOrEqual(PolyType oldT, PolyType newT) {
        if (oldT == newT) return true;
        // Allow only classic integer -> numeric widening (conservative)
        return isInt(oldT) && isNumeric(newT);
    }

    private static boolean isInt(PolyType t) {
        return t == PolyType.TINYINT
                || t == PolyType.SMALLINT
                || t == PolyType.INTEGER
                || t == PolyType.BIGINT;
    }

    private static boolean isNumeric(PolyType t) {
        return isInt(t)
                || t == PolyType.DECIMAL
                || t == PolyType.FLOAT
                || t == PolyType.REAL
                || t == PolyType.DOUBLE;
    }

    // ---------- Helpers ----------

    private static boolean isNoopRoot(DocumentSchema s) {
        DocumentSchema.ObjectNode r = s.root();
        return r.properties.isEmpty()
                && r.additionalProperties == DocumentSchema.AdditionalProperties.ALLOW;
    }
}
