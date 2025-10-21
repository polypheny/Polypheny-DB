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
     * Root-only additionalProperties semantics:
     * - You may NOT add new properties (all declared props are required).
     * - You may remove properties ONLY if the proposed schema's root additionalProperties == ALLOW
     *   (global toggle, applies to every object level).
     * - You may not tighten additionalProperties (ALLOW -> FORBID).
     * - Scalars: only widening allowed (int -> numeric). Equal types are OK.
     * - Arrays: items must be compatible; minItems must NOT increase; uniqueItems must NOT turn from false->true.
     * - Object/Array/Scalar node kinds must not change.
     */
    public static boolean isCompatible(@Nullable DocumentSchema current, @Nullable DocumentSchema proposed) {
        if (proposed == null) return true;
        if (current == null) return false; // conservative: force preflight on ALTER

        // Root-level AP tightening ALLOW -> FORBID is unsafe (requires preflight)
        if (current.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW
                && proposed.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
            return false;
        }

        // We need to know if the *proposed* root allows extras to decide if dropping fields is safe.
        boolean proposedAllowsExtras = (proposed.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW);
        return isObjectCompatible(current.root(), proposed.root(), proposedAllowsExtras);
    }

    private static boolean isObjectCompatible(
            DocumentSchema.ObjectNode cur,
            DocumentSchema.ObjectNode prop,
            boolean proposedAllowsExtras) {

        // Adding properties? (proposed has a key that current does not) -> not allowed (adds a required field)
        for (Map.Entry<String, DocumentSchema.Node> e : prop.properties.entrySet()) {
            String k = e.getKey();
            DocumentSchema.Node curChild = cur.properties.get(k);
            if (curChild == null) {
                return false; // adding new required field
            }
            if (!isNodeCompatible(curChild, e.getValue(), proposedAllowsExtras)) {
                return false;
            }
        }

        // Removing properties is safe only if the proposed root allows extras
        if (!proposedAllowsExtras) {
            for (String k : cur.properties.keySet()) {
                if (!prop.properties.containsKey(k)) {
                    return false; // would turn existing data into extras while extras are FORBID
                }
            }
        }

        return true;
    }

    private static boolean isNodeCompatible(
            DocumentSchema.Node cur,
            DocumentSchema.Node prop,
            boolean proposedAllowsExtras) {

        if (cur instanceof DocumentSchema.ScalarNode cs && prop instanceof DocumentSchema.ScalarNode ps) {
            return isScalarWideningOrEqual(cs.type, ps.type);
        }
        if (cur instanceof DocumentSchema.ObjectNode co && prop instanceof DocumentSchema.ObjectNode po) {
            return isObjectCompatible(co, po, proposedAllowsExtras);
        }
        if (cur instanceof DocumentSchema.ArrayNode ca && prop instanceof DocumentSchema.ArrayNode pa) {
            return isArrayCompatible(ca, pa, proposedAllowsExtras);
        }
        // Changing node kind (scalar<->object/array, object<->array) is unsafe
        return false;
    }

    private static boolean isArrayCompatible(
            DocumentSchema.ArrayNode cur,
            DocumentSchema.ArrayNode prop,
            boolean proposedAllowsExtras) {

        // items schema must be compatible (propagate same AP policy)
        if (!isNodeCompatible(cur.items, prop.items, proposedAllowsExtras)) return false;

        // minItems must NOT increase
        int curMin  = (cur.minItems  == null ? 0 : cur.minItems);
        int propMin = (prop.minItems == null ? 0 : prop.minItems);
        if (propMin > curMin) return false;

        // uniqueItems must NOT turn false/null -> true
        boolean curUnique  = Boolean.TRUE.equals(cur.uniqueItems);
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
                && s.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW;
    }
}
