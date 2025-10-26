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

package org.polypheny.db.languages.mql2alg;

import com.mongodb.lang.Nullable;
import org.bson.*;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.mql.MqlInsert;
import org.polypheny.db.languages.mql.MqlUpdate;
import org.polypheny.db.schema.document.DocumentSchema;
import org.polypheny.db.schema.document.EnforcementMode;
import org.polypheny.db.schema.document.SchemaJson;
import org.polypheny.db.schema.document.SchemaMeta;
import org.polypheny.db.schema.document.SchemaValidator;
import org.polypheny.db.type.PolyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;
import java.util.Set;


/**
 * Centralized schema enforcement for MQL data-changing statements.
 * Keeps MqlToAlgConverter free from validation details.
 */
final class MqlSchemaEnforcer {

    private static final Logger LOG = LoggerFactory.getLogger(MqlSchemaEnforcer.class);

    // ---------- Public entry points ----------

    void validateInsert( MqlInsert query, Entity entity) {
        var metaOpt = loadSchemaMeta(entity);
        if (metaOpt.isEmpty()) return;

        EnforcementMode mode = resolveMode(metaOpt.get());
        if (mode == EnforcementMode.OFF) return;

        DocumentSchema schema = parseSchemaOrThrow(metaOpt.get().schemaJson);

        for ( BsonValue v : query.getValues()) {
            BsonDocument d = v.asDocument();

            // do not force an _id unless modeled
            BsonDocument toCheck = d;
            if (d.containsKey( DocumentType.DOCUMENT_ID)) {
                toCheck = d.clone();
                toCheck.remove(DocumentType.DOCUMENT_ID);
            }

            if (!SchemaValidator.conformsTo(schema, toCheck)) {
                handle(mode,
                        "Inserted document does not conform to the collection schema.",
                        entity.getName(), d);
            }
        }
    }

    void validateUpdate( MqlUpdate query, Entity entity) {
        var metaOpt = loadSchemaMeta(entity);
        if (metaOpt.isEmpty()) return;

        EnforcementMode mode = resolveMode(metaOpt.get());
        if (mode == EnforcementMode.OFF) return;

        DocumentSchema schema = parseSchemaOrThrow(metaOpt.get().schemaJson);

        // A) Replacement update: whole-document validation
        if (!query.isUsesPipeline()
                && query.getUpdate() != null
                && query.getUpdate().isDocument()
                && isReplacementUpdate(query.getUpdate().asDocument())) {

            BsonDocument repl = query.getUpdate().asDocument();
            BsonDocument toCheck = repl;
            if (toCheck.containsKey(DocumentType.DOCUMENT_ID)) {
                toCheck = toCheck.clone();
                toCheck.remove(DocumentType.DOCUMENT_ID);
            }

            if (!SchemaValidator.conformsTo(schema, toCheck)) {
                handle(mode,
                        "Replacement document does not conform to the collection schema.",
                        entity.getName(), repl);
            }
            return;
        }

        // B) Operator or pipeline update:
        // We can't evaluate expressions at plan-time, but we can block the
        // creation of unknown TOP-LEVEL fields when AP = FORBID.
        if (schema.additionalProperties() == DocumentSchema.AdditionalProperties.ALLOW) return;

        Set<String> allowedTop = schema.root().properties.keySet();

        if (query.isUsesPipeline()) {
            for (BsonValue stageVal : query.getPipeline()) {
                if (!stageVal.isDocument() || stageVal.asDocument().size() != 1) continue;
                BsonDocument stage = stageVal.asDocument();
                String op = stage.getFirstKey();
                if (!stage.get(op).isDocument()) continue;
                BsonDocument body = stage.getDocument(op);

                switch (op) {
                    case "$set":
                    case "$addFields":
                        for (var e : body.entrySet()) {
                            // forbid unknown top-level fields if AP=FORBID
                            gateTopLevel(allowedTop, e.getKey(), mode, entity, op);
                            // NEW: check literal value types against schema
                            checkSetLiteralAgainstSchema(schema, mode, entity, e.getKey(), e.getValue(), op);
                        }
                        break;
                    case "$project":
                    case "$unset":
                        // removing/reshaping: fine
                        break;
                    default:
                        // not used in reduced update pipeline path you support
                        break;
                }
            }
        } else if (query.getUpdate() != null && query.getUpdate().isDocument()) {
            BsonDocument up = query.getUpdate().asDocument();
            for (var e : up.entrySet()) {
                String op = e.getKey();
                if (!e.getValue().isDocument()) continue;
                BsonDocument body = e.getValue().asDocument();

                switch (op) {
                    case "$set":
                        for (var kv : body.entrySet()) {
                            gateTopLevel(allowedTop, kv.getKey(), mode, entity, op);
                            // NEW: catch e.g. $set: { age: "5" } when schema says NUMBER
                            checkSetLiteralAgainstSchema(schema, mode, entity, kv.getKey(), kv.getValue(), op);
                        }
                        break;
                    case "$inc":
                    case "$min":
                    case "$max":
                        for (var kv : body.entrySet()) {
                            gateTopLevel(allowedTop, kv.getKey(), mode, entity, op);
                            // NEW: numeric target field check (schema must say NUMBER)
                            checkNumericTarget(schema, mode, entity, kv.getKey(), op);
                            // (optional) also ensure the provided value is numeric:
                            if (!kv.getValue().isNumber()) {
                                handle(mode, "Operator " + op + " requires a numeric value", entity.getName(), kv.getValue());
                            }
                        }
                        break;
                    case "$mul":
                    case "$currentDate":
                        for (var kv : body.entrySet()) {
                            gateTopLevel(allowedTop, kv.getKey(), mode, entity, op);
                            // NEW: DATE/TIMESTAMP target check
                            checkCurrentDateTarget(schema, mode, entity, kv.getKey());
                        }
                        break;
                    case "$addToSet":
                        for (var kv : body.entrySet()) {
                            gateTopLevel(allowedTop, kv.getKey(), mode, entity, op);
                        }
                        break;

                    case "$rename":
                        for (var kv : body.entrySet()) {
                            if (kv.getValue().isString()) {
                                gateTopLevel(allowedTop, kv.getValue().asString().getValue(), mode, entity, op);
                            }
                        }
                        break;

                    case "$unset":
                        // removing is fine
                        break;

                    default:
                        // unsupported ops are handled elsewhere
                        break;
                }
            }
        }
    }

    // ---------- Helpers ----------

    private static Optional<SchemaMeta> loadSchemaMeta(Entity entity) {
        if (!(entity instanceof LogicalCollection lc)) return Optional.empty();
        return SchemaMeta.readCurrent( Catalog.getInstance(), lc.namespaceId, lc.id);
    }

    private static EnforcementMode resolveMode(SchemaMeta meta) {
        try {
            return EnforcementMode.valueOf(
                    (meta.enforcement == null ? "OFF" : meta.enforcement).trim().toUpperCase());
        } catch (IllegalArgumentException iae) {
            return EnforcementMode.OFF;
        }
    }

    private static DocumentSchema parseSchemaOrThrow(String json) {
        try {
            DocumentSchema s = SchemaJson.parse(json);
            s.validateOrThrow();
            return s;
        } catch (Exception e) {
            throw new GenericRuntimeException("Stored collection schema is invalid", e);
        }
    }

    private static boolean isReplacementUpdate(BsonDocument upd) {
        // true iff at least one top-level key does NOT start with '$'
        for (String k : upd.keySet()) {
            if (!k.startsWith("$")) return true;
        }
        return false;
    }

    private static String topLevelSegment(String path) {
        int dot = path.indexOf('.');
        return dot < 0 ? path : path.substring(0, dot);
    }

    private static void gateTopLevel(
            Set<String> allowedTop,
            String path,
            EnforcementMode mode,
            Entity entity,
            String op) {
        String top = topLevelSegment(path);
        if (!allowedTop.contains(top)) {
            String msg = "Schema forbids introducing unknown top-level field '" + top + "' via " + op;
            handle(mode, msg, entity.getName(), path);
        }
    }

    private static void handle(EnforcementMode mode, String msg, String entity, Object detail) {
        switch (mode) {
            case STRICT -> throw new GenericRuntimeException(msg);
            case WARN -> LOG.warn("{}; allowed due to WARN. Entity='{}' Detail={}", msg, entity, summarize(detail));
            case OFF -> { /* no-op */ }
        }
    }

    private static String summarize(@Nullable Object value) {
        try {
            String s = String.valueOf(value);
            return s.length() > 500 ? s.substring(0, 500) + "…" : s;
        } catch (Exception e) {
            return "<unprintable>";
        }
    }

    // ---- Path & type helpers ----------------------------------------------------

    private static Optional<DocumentSchema.Node> resolveNode(DocumentSchema schema, String path) {
        DocumentSchema.Node cur = schema.root();
        if (path == null || path.isEmpty()) return Optional.empty();

        String[] segs = path.split("\\.");
        for (int i = 0; i < segs.length; i++) {
            String seg = segs[i];

            if (cur instanceof DocumentSchema.ObjectNode on) {
                DocumentSchema.Node nxt = on.properties.get(seg);
                if (nxt == null) return Optional.empty();
                cur = nxt;

            } else if (cur instanceof DocumentSchema.ArrayNode an) {
                // step into items; allow numeric index segments (e.g. "tags.0")
                cur = an.items;
                if (seg.matches("\\d+")) {
                    // consumed an index; continue
                } else {
                    if (cur instanceof DocumentSchema.ObjectNode aon) {
                        DocumentSchema.Node nxt = aon.properties.get(seg);
                        if (nxt == null) return Optional.empty();
                        cur = nxt;
                    } else {
                        return Optional.empty();
                    }
                }

            } else {
                // scalar cannot have children
                return Optional.empty();
            }
        }
        return Optional.ofNullable(cur);
    }

    /** Minimal scalar compatibility check (aligned with SchemaValidator). */
    private static boolean matchesScalar(BsonValue v, PolyType t) {
        if (t == PolyType.ANY)  return true;
        if (t == PolyType.NULL) return (v == null || v.isNull());

        switch (t) {
            case BOOLEAN:
                return v instanceof BsonBoolean;

            // strings
            case CHAR:
            case VARCHAR:
            case TEXT:
            case JSON:
                return v instanceof BsonString;

            // numerics
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return (v instanceof BsonInt32) || (v instanceof BsonInt64);
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return (v instanceof BsonNumber)
                        || (v instanceof BsonDouble)
                        || (v instanceof BsonInt32)
                        || (v instanceof BsonInt64);

            // collections/doc
            case ARRAY:    return v instanceof BsonArray;
            case DOCUMENT:
            case MAP:      return v instanceof BsonDocument;

            // temporal
            case DATE:       return v instanceof BsonDateTime;
            case TIMESTAMP:  return v instanceof BsonTimestamp;

            // binary-ish
            case BINARY:
            case VARBINARY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case AUDIO:
                return v instanceof BsonBinary;

            default:
                return false;
        }
    }

    private static boolean isNumericPoly(PolyType t) {
        return switch (t) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, REAL, DOUBLE -> true;
            default -> false;
        };
    }

    private static void checkSetLiteralAgainstSchema(
            DocumentSchema schema,
            EnforcementMode mode,
            Entity entity,
            String path,
            BsonValue value,
            String op // "$set" or "$addFields" in pipeline
    ) {
        var nodeOpt = resolveNode(schema, path);

        // Unknown path under FORBID? Fail/warn.
        if (nodeOpt.isEmpty()) {
            if (schema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
                handle(mode, "Schema forbids setting unknown field '" + path + "'", entity.getName(), path);
            }
            return; // ALLOW → OK
        }

        var node = nodeOpt.get();

        // Only literal checks here (documents/arrays may be expressions)
        if (value.isDocument() || value.isArray()) {
            return;
        }

        if (node instanceof DocumentSchema.ScalarNode sn) {
            if (!matchesScalar(value, sn.type)) {
                handle(mode,
                        "Value for '" + path + "' does not match schema type " + sn.type,
                        entity.getName(),
                        value);
            }
        } else if (node instanceof DocumentSchema.ObjectNode) {
            if (!value.isDocument()) {
                handle(mode, "Value for object field '" + path + "' must be a document", entity.getName(), value);
            }
        } else if (node instanceof DocumentSchema.ArrayNode) {
            if (!value.isArray()) {
                handle(mode, "Value for array field '" + path + "' must be an array", entity.getName(), value);
            }
        }
    }

    private static void checkNumericTarget(
            DocumentSchema schema,
            EnforcementMode mode,
            Entity entity,
            String path,
            String op // "$inc", "$mul", "$min", "$max"
    ) {
        var nodeOpt = resolveNode(schema, path);
        if (nodeOpt.isEmpty()) {
            if (schema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
                handle(mode, "Schema forbids updating unknown field '" + path + "' via " + op, entity.getName(), path);
            }
            return;
        }
        var node = nodeOpt.get();
        if (node instanceof DocumentSchema.ScalarNode sn) {
            if (!isNumericPoly(sn.type)) {
                handle(mode, "Operator " + op + " requires numeric field, but '" + path + "' is " + sn.type, entity.getName(), path);
            }
        } else {
            handle(mode, "Operator " + op + " requires numeric scalar field, but '" + path + "' is not scalar", entity.getName(), path);
        }
    }

    private static void checkCurrentDateTarget(
            DocumentSchema schema,
            EnforcementMode mode,
            Entity entity,
            String path
    ) {
        var nodeOpt = resolveNode(schema, path);
        if (nodeOpt.isEmpty()) {
            if (schema.additionalProperties() == DocumentSchema.AdditionalProperties.FORBID) {
                handle(mode, "Schema forbids updating unknown field '" + path + "' via $currentDate", entity.getName(), path);
            }
            return;
        }
        var node = nodeOpt.get();
        if (node instanceof DocumentSchema.ScalarNode sn) {
            var t = sn.type;
            if (!(t == PolyType.DATE || t == PolyType.TIMESTAMP)) {
                handle(mode, "Operator $currentDate requires DATE or TIMESTAMP field, but '" + path + "' is " + t, entity.getName(), path);
            }
        } else {
            handle(mode, "Operator $currentDate requires DATE or TIMESTAMP scalar field, but '" + path + "' is not scalar", entity.getName(), path);
        }
    }


}
