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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class SchemaEnforcer {

    private final Optional<SchemaMeta> meta;
    private final Optional<DocumentSchema> schema;
    private final EnforcementMode mode;

    private SchemaEnforcer(Optional<SchemaMeta> meta, Optional<DocumentSchema> schema, EnforcementMode mode) {
        this.meta = meta;
        this.schema = schema;
        this.mode = mode;
    }

    /** Create an enforcer bound to a collection entity (no-ops if no schema). */
    public static SchemaEnforcer forEntity( Entity entity) {
        Optional<SchemaMeta> m = Optional.empty();
        if (entity instanceof LogicalCollection lc) {
            m = SchemaMeta.readCurrent( Catalog.getInstance(), lc.namespaceId, lc.id);
        }
        Optional<DocumentSchema> s = m.map(mm -> SchemaJson.parse(mm.schemaJson)); // uses your DocumentSchema/SchemaJson
        EnforcementMode em = m.map(mm -> safeMode(mm.enforcement)).orElse(EnforcementMode.OFF);
        return new SchemaEnforcer(m, s, em);
    }

    public EnforcementMode mode() { return mode; }
    public boolean active() { return schema.isPresent() && mode != EnforcementMode.OFF; }

    /** STRICT: every doc must conform. WARN: log only. OFF: skip. */
    public void validateInsertDocs( List<BsonDocument> docs) {
        if (!active()) return;
        for (BsonDocument d : docs) {
            BsonDocument toCheck = stripInternalId(d);
            if (!SchemaValidator.conformsTo(schema.get(), toCheck)) {
                handleViolation("Inserted document does not conform to the collection schema.", d);
            }
        }
    }

    /** STRICT: replacement must conform. */
    public void validateReplacement(BsonDocument replacement) {
        if (!active()) return;
        BsonDocument toCheck = stripInternalId(replacement);
        if (!SchemaValidator.conformsTo(schema.get(), toCheck)) {
            handleViolation("Replacement document does not conform to the collection schema.", replacement);
        }
    }

    /**
     * Validate UPDATE pieces we can check statically:
     * - when additionalProperties=FORBID, the written/renamed paths must exist in schema
     * - literal RHS type checks via probe doc
     */
    public void validateUpdateOps(
            Map<String, BsonValue> literalAssignments,
            Collection<String> unsetPaths,
            Map<String,String> renames) {
        if (!active()) return;

        DocumentSchema s = schema.get();
        boolean forbidAP = s.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID;

        if (forbidAP) {
            for (String p : literalAssignments.keySet()) ensurePathAllowed(s, p);
            for (String p : renames.values())            ensurePathAllowed(s, p);
            // $unset always ok
        }

        // type checks for literals by probing just that path
        for (var e : literalAssignments.entrySet()) {
            BsonDocument probe = new BsonDocument();
            putPathLiteral(probe, e.getKey(), e.getValue());
            if (!SchemaValidator.conformsTo(s, probe)) {
                handleViolation("Update literal at '" + e.getKey() + "' violates schema.", e.getValue());
            }
        }
    }

    // ---- helpers ----

    private static EnforcementMode safeMode(String s) {
        if (s == null) return EnforcementMode.OFF;
        try { return EnforcementMode.valueOf(s.trim().toUpperCase()); }
        catch (Exception ignore) { return EnforcementMode.OFF; }
    }

    private static BsonDocument stripInternalId(BsonDocument d) {
        if (d.containsKey( DocumentType.DOCUMENT_ID)) {
            BsonDocument c = d.clone(); c.remove(DocumentType.DOCUMENT_ID); return c;
        }
        return d;
    }

    private void ensurePathAllowed(DocumentSchema schema, String dotted) {
        String[] parts = dotted.split("\\.");
        DocumentSchema.Node cur = schema.root();
        for (String p : parts) {
            if (!(cur instanceof DocumentSchema.ObjectNode on)) {
                handleViolation("Unknown path '"+dotted+"' (not an object)", dotted);
                return;
            }
            DocumentSchema.Node next = on.properties.get(p);
            if (next == null) {
                handleViolation("Unknown field '"+p+"' in path '"+dotted+"'", dotted);
                return;
            }
            cur = next;
        }
    }

    private static void putPathLiteral(BsonDocument root, String dotted, BsonValue val) {
        String[] parts = dotted.split("\\.");
        BsonDocument cur = root;
        for (int i = 0; i < parts.length - 1; i++) {
            cur = cur.computeIfAbsent(parts[i], k -> new BsonDocument()).asDocument();
        }
        cur.put(parts[parts.length - 1], val);
    }

    private void handleViolation(String msg, Object sample) {
        if (mode == EnforcementMode.STRICT) {
            throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException(msg);
        } else {
            org.slf4j.LoggerFactory.getLogger(SchemaEnforcer.class)
                    .warn("{} (WARN mode). Sample={}", msg, summarize(sample));
        }
    }

    private static String summarize(Object v) {
        try {
            String s = String.valueOf(v);
            return s.length() > 400 ? s.substring(0, 400) + "…" : s;
        } catch (Exception e) { return "<unprintable>"; }
    }
}
