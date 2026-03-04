/*
 * Copyright 2019-2026 The Polypheny Project
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
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.schema.document.DocumentSchema.AllOfNode;
import org.polypheny.db.schema.document.DocumentSchema.AnyOfNode;
import org.polypheny.db.schema.document.DocumentSchema.ArrayNode;
import org.polypheny.db.schema.document.DocumentSchema.Node;
import org.polypheny.db.schema.document.DocumentSchema.NotNode;
import org.polypheny.db.schema.document.DocumentSchema.ObjectNode;
import org.polypheny.db.schema.document.DocumentSchema.OneOfNode;
import org.polypheny.db.schema.document.DocumentSchema.ScalarNode;
import org.polypheny.db.type.PolyType;

/**
 * Fast compatibility heuristic for schema evolution.
 *
 * <p>If this returns {@code true}, the engine can often skip a full preflight scan.
 * If it returns {@code false}, a scan is required to avoid invalidating existing documents.</p>
 *
 * <p>This implementation is conservative, especially for composition nodes.</p>
 */
public final class SchemaCompatibility {

    private SchemaCompatibility() {
    }

    public static boolean isCompatible( @Nullable DocumentSchema current, @Nullable DocumentSchema proposed ) {
        if ( proposed == null ) {
            return true;
        }
        if ( current == null ) {
            return false; // force preflight when no current schema
        }

        DocumentSchema.AdditionalProperties curRootAp =
                current.additionalProperties() != null ? current.additionalProperties() : DocumentSchema.AdditionalProperties.ALLOW;
        DocumentSchema.AdditionalProperties propRootAp =
                proposed.additionalProperties() != null ? proposed.additionalProperties() : curRootAp;

        // Root-level AP tightening is unsafe (requires preflight)
        if ( curRootAp == DocumentSchema.AdditionalProperties.ALLOW
                && propRootAp == DocumentSchema.AdditionalProperties.FORBID ) {
            return false;
        }

        return isObjectCompatible(current.root(), proposed.root(), curRootAp, propRootAp);
    }

    private static boolean isObjectCompatible(
            ObjectNode cur,
            ObjectNode prop,
            DocumentSchema.AdditionalProperties inheritedCurAp,
            DocumentSchema.AdditionalProperties inheritedPropAp ) {

        DocumentSchema.AdditionalProperties curAp = effectiveAp(cur.additionalProperties, inheritedCurAp);
        DocumentSchema.AdditionalProperties propAp = effectiveAp(prop.additionalProperties, inheritedPropAp);

        // requiredness comparison (explicit required vs default-all)
        Set<String> curReq = cur.effectiveRequired();
        Set<String> propReq = prop.effectiveRequired();

        // New required fields are unsafe if not previously required
        for ( String k : propReq ) {
            if ( !curReq.contains(k) ) {
                // field newly required
                return false;
            }
        }

        // Properties present in proposed
        for ( Map.Entry<String, Node> e : prop.properties.entrySet() ) {
            String k = e.getKey();
            Node curChild = cur.properties.get(k);

            if ( curChild == null ) {
                // Field added. Safe ONLY if optional in proposed.
                boolean newlyRequired = propReq.contains(k);
                if ( newlyRequired ) {
                    return false;
                }
                // Optional field addition is OK.
                continue;
            }

            if ( !isNodeCompatible(curChild, e.getValue(), curAp, propAp) ) {
                return false;
            }
        }

        // Properties removed in proposed: safe only if proposed allows extras at that object
        if ( propAp == DocumentSchema.AdditionalProperties.FORBID ) {
            for ( String k : cur.properties.keySet() ) {
                if ( !prop.properties.containsKey(k) ) {
                    return false;
                }
            }
        }

        // Tightening per-object additionalProperties from allow->forbid is unsafe
        if ( curAp == DocumentSchema.AdditionalProperties.ALLOW && propAp == DocumentSchema.AdditionalProperties.FORBID ) {
            return false;
        }

        // Object size constraints tightening
        if ( tightensLowerBound(cur.minProperties, prop.minProperties) ) {
            return false;
        }
        if ( tightensUpperBound(cur.maxProperties, prop.maxProperties) ) {
            return false;
        }

        return true;
    }

    private static boolean isNodeCompatible(
            Node cur,
            Node prop,
            DocumentSchema.AdditionalProperties inheritedCurAp,
            DocumentSchema.AdditionalProperties inheritedPropAp ) {

        // Composition nodes are conservative: force preflight
        if ( cur instanceof AnyOfNode || cur instanceof OneOfNode || cur instanceof AllOfNode || cur instanceof NotNode ) {
            return false;
        }
        if ( prop instanceof AnyOfNode || prop instanceof OneOfNode || prop instanceof AllOfNode || prop instanceof NotNode ) {
            return false;
        }

        if ( cur instanceof ScalarNode cs && prop instanceof ScalarNode ps ) {
            return isScalarCompatible(cs, ps);
        }

        if ( cur instanceof ObjectNode co && prop instanceof ObjectNode po ) {
            return isObjectCompatible(co, po, inheritedCurAp, inheritedPropAp);
        }

        if ( cur instanceof ArrayNode ca && prop instanceof ArrayNode pa ) {
            return isArrayCompatible(ca, pa, inheritedCurAp, inheritedPropAp);
        }

        // Changing node kind (scalar<->object/array, object<->array) is unsafe
        return false;
    }

    private static boolean isArrayCompatible(
            ArrayNode cur,
            ArrayNode prop,
            DocumentSchema.AdditionalProperties inheritedCurAp,
            DocumentSchema.AdditionalProperties inheritedPropAp ) {

        // items schema must be compatible (propagate AP)
        if ( !isNodeCompatible(cur.items, prop.items, inheritedCurAp, inheritedPropAp) ) {
            return false;
        }

        // minItems must NOT increase (tightening)
        if ( tightensLowerBound(cur.minItems, prop.minItems) ) {
            return false;
        }

        // maxItems must NOT decrease (tightening)
        if ( tightensUpperBound(cur.maxItems, prop.maxItems) ) {
            return false;
        }

        // uniqueItems must NOT turn false/null -> true
        boolean curUnique = Boolean.TRUE.equals(cur.uniqueItems);
        boolean propUnique = Boolean.TRUE.equals(prop.uniqueItems);
        if ( !curUnique && propUnique ) {
            return false;
        }

        return true;
    }

    // -----------------------------------------------------------------------------------------
    // Scalars
    // -----------------------------------------------------------------------------------------

    private static boolean isScalarCompatible(ScalarNode cur, ScalarNode prop) {
        // type widening: proposed types must include all current types (or numeric widening)
        if ( !isTypeSupersetOrWidening(cur.types, prop.types) ) {
            return false;
        }

        // Constraint tightening => requires scan
        if ( tightensLowerBound(cur.minLength, prop.minLength) ) {
            return false;
        }
        if ( tightensUpperBound(cur.maxLength, prop.maxLength) ) {
            return false;
        }
        if ( cur.pattern == null && prop.pattern != null ) {
            return false;
        }
        if ( cur.pattern != null && prop.pattern != null && !cur.pattern.equals(prop.pattern) ) {
            // pattern change is risky; require scan
            return false;
        }

        if ( tightensLowerBound(cur.minimum, prop.minimum) ) {
            return false;
        }
        if ( tightensUpperBound(cur.maximum, prop.maximum) ) {
            return false;
        }
        if ( cur.multipleOf == null && prop.multipleOf != null ) {
            return false;
        }
        if ( cur.multipleOf != null && prop.multipleOf != null && cur.multipleOf.compareTo(prop.multipleOf) != 0 ) {
            // changing multipleOf is risky; require scan
            return false;
        }

        // const / enum tightening
        if ( cur.constValue == null && prop.constValue != null ) {
            return false;
        }
        if ( cur.constValue != null && prop.constValue != null && !cur.constValue.equals(prop.constValue) ) {
            return false;
        }

        if ( cur.enumValues == null && prop.enumValues != null ) {
            return false;
        }
        if ( cur.enumValues != null && prop.enumValues != null ) {
            if ( !enumIsSuperset(cur.enumValues, prop.enumValues) ) {
                // prop must be a superset (relaxing). If it removes values => tightening
                return false;
            }
        }

        return true;
    }

    private static boolean enumIsSuperset(List<JsonNode> oldEnum, List<JsonNode> newEnum) {
        Set<JsonNode> newSet = new HashSet<>(newEnum);
        // If newEnum contains all old values, it's relaxing; otherwise tightening.
        return newSet.containsAll(oldEnum);
    }

    private static boolean isTypeSupersetOrWidening(List<PolyType> oldTypes, List<PolyType> newTypes) {
        if ( oldTypes == null || oldTypes.isEmpty() ) {
            return true;
        }
        if ( newTypes == null || newTypes.isEmpty() ) {
            return false;
        }

        Set<PolyType> newSet = new HashSet<>(newTypes);
        for ( PolyType ot : oldTypes ) {
            if ( newSet.contains(ot) ) {
                continue;
            }
            // allow numeric widening: old int -> new contains numeric
            if ( isInt(ot) ) {
                boolean hasNumeric = false;
                for ( PolyType nt : newTypes ) {
                    if ( isNumeric(nt) ) {
                        hasNumeric = true;
                        break;
                    }
                }
                if ( hasNumeric ) {
                    continue;
                }
            }
            return false;
        }

        return true;
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

    // -----------------------------------------------------------------------------------------
    // Bounds + AP helpers
    // -----------------------------------------------------------------------------------------

    private static DocumentSchema.AdditionalProperties effectiveAp(
            DocumentSchema.AdditionalProperties nodeAp,
            DocumentSchema.AdditionalProperties inherited ) {

        if ( nodeAp == null || nodeAp == DocumentSchema.AdditionalProperties.INHERIT ) {
            return inherited;
        }
        return nodeAp;
    }

    private static boolean tightensLowerBound(Integer cur, Integer prop) {
        int c = cur == null ? 0 : cur;
        int p = prop == null ? 0 : prop;
        return p > c;
    }

    private static boolean tightensUpperBound(Integer cur, Integer prop) {
        if ( prop == null ) {
            return false; // removing upper bound is relaxing
        }
        if ( cur == null ) {
            return true; // adding upper bound is tightening
        }
        return prop < cur;
    }

    private static boolean tightensLowerBound(BigDecimal cur, BigDecimal prop) {
        if ( prop == null ) {
            return false;
        }
        if ( cur == null ) {
            return true; // adding a minimum is tightening
        }
        return prop.compareTo(cur) > 0;
    }

    private static boolean tightensUpperBound(BigDecimal cur, BigDecimal prop) {
        if ( prop == null ) {
            return false;
        }
        if ( cur == null ) {
            return true; // adding a maximum is tightening
        }
        return prop.compareTo(cur) < 0;
    }
}
