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

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.document.DocumentSchema.Node;
import org.polypheny.db.schema.document.SchemaOptionsResolver.AlterMode;
import org.polypheny.db.transaction.Statement;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates ALTER COLLECTION SCHEMA planning + preflight policy.
 * - Computes final schema (respecting PATCH/REPLACE)
 * - Decides if a scan (preflight) is required
 * - Applies allow/deny policy based on preflight result
 */
public final class SchemaAlterEngine {

    public record Plan(
            DocumentSchema currentSchema,           // may be null if none persisted
            EnforcementMode currentMode,
            DocumentSchema finalSchema,             // null for enforcement-only update
            EnforcementMode finalMode,
            boolean isPatch,
            boolean needsPreflight                  // engine decision
    ) {}

    public record Outcome(boolean applied, SchemaAlterPreflightReport preflight) {}

    public SchemaAlterEngine() {}

    public Plan plan(
            final SchemaOptionsResolver.Resolved r,
            final DocumentSchema current,
            final EnforcementMode currentMode) {

        final EnforcementMode targetMode = (r.mode != null) ? r.mode : currentMode;

        // Enforcement-only change?
        if (r.schema == null) {
            final boolean needsPreflight = (current != null) && isTightening(currentMode, targetMode);
            return new Plan(current, currentMode, null, targetMode, false, needsPreflight);
        }

        // Build final schema (PATCH or REPLACE)
        final DocumentSchema finalSchema =
                (r.alterMode == AlterMode.PATCH && current != null)
                        ? mergePatch(current, r.schema)
                        : requireNonNull(r.schema, "schema");

        finalSchema.validateOrThrow();

        // ---------- Decide preflight ----------
        boolean needsPreflight;

        if (current == null) {
            // If there is data, introducing a schema could invalidate docs.
            // Be conservative: force preflight (scan) when adding a schema to an existing collection.
            needsPreflight = true;
        } else {
            // Compatibility heuristic
            needsPreflight = !SchemaCompatibility.isCompatible(current, finalSchema);

            // EXTRA SAFETY: force preflight on AP ALLOW -> FORBID even if a bug ever sneaks into compatibility.
            if (current.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW
                    && finalSchema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
                needsPreflight = true;
            }
        }

        return new Plan(current, currentMode, finalSchema, targetMode, r.alterMode == AlterMode.PATCH, needsPreflight);
    }

    /**
     * Run preflight if the plan requires it. No-op if not required or schema is null.
     */
    public SchemaAlterPreflightReport preflightIfRequired(
            final Catalog catalog,
            final LogicalCollection coll,
            final Plan plan,
            final Statement stmt) {

        if (!plan.needsPreflight() || plan.finalSchema() == null) {
            return new SchemaAlterPreflightReport(true, 0, 0, java.util.List.of());
        }

        // If provably empty, skip scan.
        if (SchemaAlterPreflight.canProveEmpty(catalog, coll, stmt)) {
            return new SchemaAlterPreflightReport(true, 0, 0, java.util.List.of());
        }

        return SchemaAlterPreflight.run(catalog, coll, plan.finalSchema(), stmt);
    }

    /**
     * Enforcement-only preflight (tightening to STRICT).
     */
    public SchemaAlterPreflightReport preflightForEnforcementOnlyIfRequired(
            final Catalog catalog,
            final LogicalCollection coll,
            final Plan plan,
            final Statement stmt) {

        final EnforcementMode current = plan.currentMode();
        final EnforcementMode target  = plan.finalMode();
        if (!isTightening(current, target)) {
            return new SchemaAlterPreflightReport(true, 0, 0, List.of());
        }

        // Need a persisted schema to validate against
        Optional<SchemaMeta> metaOpt = SchemaMeta.readCurrent(catalog, coll.namespaceId, coll.id);

        if (metaOpt.isEmpty()) {
            // You already reject changing validation without a schema elsewhere
            return new SchemaAlterPreflightReport(true, 0, 0, List.of());
        }

        DocumentSchema currentSchema = SchemaJson.parse(metaOpt.get().schemaJson);
        return SchemaAlterPreflight.run(catalog, coll, currentSchema, stmt);
    }

    /**
     * Apply allow/deny policy given preflight result.
     * Throws on denial with a descriptive message.
     */
    public Outcome applyPolicyOrThrow(
            final SchemaAlterPreflightReport rep,
            final boolean isSchemaChange,
            final EnforcementMode currentMode,
            final EnforcementMode finalMode) {

        if (isSchemaChange) {
            if (!rep.ok) {
                throw new GenericRuntimeException(
                        String.format(
                                "ALTER SCHEMA would invalidate %d/%d documents; examples: %s",
                                rep.failing, rep.scanned, rep.compactSummary(5)));
            }
            return new Outcome(true, rep);
        }

        if (isTightening(currentMode, finalMode) && !rep.ok) {
            throw new GenericRuntimeException(
                    String.format(
                            "Cannot set validationAction=STRICT: %d/%d documents violate the current schema; examples: %s",
                            rep.failing, rep.scanned, rep.compactSummary(5)));
        }

        return new Outcome(true, rep);
    }

    // ---------- helpers ----------

    private static boolean isTightening(final EnforcementMode cur, final EnforcementMode fin) {
        if (fin == null) return false;
        if (cur == EnforcementMode.STRICT) return false;
        return fin == EnforcementMode.STRICT;
    }

    private static SchemaAlterPreflightReport ok() {
        return new SchemaAlterPreflightReport(true, 0, 0, java.util.List.of());
    }

    /**
     * Deep-merge PATCH: object properties merged by key; additionalProperties overridden if provided.
     */
    public static DocumentSchema mergePatch(final DocumentSchema current, final DocumentSchema patch) {
        // merge properties trees; choose AP at root
        DocumentSchema.ObjectNode mergedRoot = mergeObject(current.root(), patch.root());
        DocumentSchema.AdditionalProperties ap =
                patch.additionalProperties() != null ? patch.additionalProperties() : current.additionalProperties();
        return new DocumentSchema(mergedRoot, ap);
    }


    private static DocumentSchema.ObjectNode mergeObject(
            final DocumentSchema.ObjectNode cur,
            final DocumentSchema.ObjectNode p) {
        if (p == null) return cur;

        Map<String, DocumentSchema.Node> props = new LinkedHashMap<>(cur.properties);
        for (var e : p.properties.entrySet()) {
            String k = e.getKey();
            DocumentSchema.Node pn = e.getValue();
            DocumentSchema.Node cn = cur.properties.get(k);
            if (pn instanceof DocumentSchema.ObjectNode && cn instanceof DocumentSchema.ObjectNode) {
                props.put(k, mergeObject((DocumentSchema.ObjectNode) cn, (DocumentSchema.ObjectNode) pn));
            } else {
                props.put(k, pn);
            }
        }
        return new DocumentSchema.ObjectNode(props);
    }
}
