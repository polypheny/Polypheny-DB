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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonDocument;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.schema.document.SchemaValidator.Violation;
import org.polypheny.db.transaction.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;


public final class SchemaAlterPreflight {

    private SchemaAlterPreflight() {}

    private static final ObjectMapper OM = new ObjectMapper();

    public static SchemaAlterPreflightReport run(
            final Catalog catalog,
            final LogicalCollection coll,
            final DocumentSchema schema,
            final Statement stmt) {

        final var snap = catalog.getSnapshot();
        final List<AllocationEntity> allocs = new ArrayList<>(snap.alloc().getFromLogical(coll.id));
        if (allocs.isEmpty()) {
            return new SchemaAlterPreflightReport(true, 0, 0, List.of());
        }

        final LongAdder scanned = new LongAdder();
        final LongAdder failing = new LongAdder();
        final List<Violation> sample = new ArrayList<>(16);
        boolean sawReadablePlacement = false;

        for (AllocationEntity ae : allocs) {
            final Optional<DataStore<?>> optStore = AdapterManager.getInstance().getStore(ae.adapterId);
            if (optStore.isEmpty()) {
                continue;
            }
            final DataStore<?> store = optStore.get();
            sawReadablePlacement = true;

            final AllocationCollection alloc = ae.unwrapOrThrow(AllocationCollection.class);

            // The store streams JSON strings (some stores may wrap or double-encode).
            final Consumer<CharSequence> sink = (CharSequence json) -> {
                try {
                    String raw = json.toString();
                    Unwrapped u = unwrapPossibleWrappers(raw); // robust against PolyValue/quoted JSON

                    // If this doesn't even look like a document/array, ignore this fragment.
                    if (!u.looksLikeDoc) {
                        return;
                    }

                    // Parse only now; count "scanned" after successful parse.
                    BsonDocument d = BsonDocument.parse(u.json);
                    scanned.increment();

                    var res = SchemaValidator.validate(schema, d);
                    if (!res.ok()) {
                        failing.increment();
                        if (sample.size() < 16) {
                            var vs = res.violations();
                            int room = 16 - sample.size();
                            sample.addAll(vs.subList(0, Math.min(vs.size(), room)));
                        }
                    }
                } catch (Exception e) {
                    // Only treat as a failure if the fragment looked like a document but couldn't be parsed.
                    // Otherwise it's likely a non-document column/value streamed by the store; skip it.
                    String s = json.toString().trim();
                    boolean lookedLikeDoc = (s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"));
                    if (lookedLikeDoc) {
                        failing.increment();
                        if (sample.size() < 16) {
                            sample.add(new SchemaValidator.Violation("$", "parse", e.getMessage()));
                        }
                    }
                }
            };

            store.streamCollectionAsJson(alloc, sink, stmt);
        }

        if (!sawReadablePlacement) {
            // If nothing was readable, treat as OK (engine typically guards this earlier).
            return new SchemaAlterPreflightReport(true, 0, 0, List.of());
        }

        final long scannedCount = scanned.sum();
        final long failingCount = failing.sum();
        return new SchemaAlterPreflightReport(failingCount == 0, scannedCount, failingCount, sample);
    }

    public static boolean canProveEmpty(
            final Catalog catalog,
            final LogicalCollection coll,
            final Statement stmt) {
        var snap = catalog.getSnapshot();
        List<AllocationEntity> allocs = new ArrayList<>(snap.alloc().getFromLogical(coll.id));
        return allocs.isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // Unwrap helpers
    // --------------------------------------------------------------------------------------------

    /** Small holder for unwrap result. */
    private static final class Unwrapped {
        final String json;
        final boolean looksLikeDoc;
        Unwrapped(String json, boolean looksLikeDoc) {
            this.json = json;
            this.looksLikeDoc = looksLikeDoc;
        }
    }

    /**
     * Accepts:
     *  - raw JSON document:         {"name":{...},"age":22}         -> returned as-is
     *  - JSON string literal:       "\"{...}\""                     -> unquoted to {...}
     *  - PolyValue-like wrapper:    {"@type":...,"value":"{...}"}   -> extracts value
     * If the end result does not look like a JSON object/array, marks it as not-a-document.
     */
    private static Unwrapped unwrapPossibleWrappers(String s) {
        if (s == null) return new Unwrapped(null, false);
        String trimmed = s.trim();

        // Quick path: looks like an object/array
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
                (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            // Common PolyValue wrapper: {"@type": "...", "value": "...json..."}
            if (trimmed.startsWith("{\"@type\"") && trimmed.contains("\"value\"")) {
                try {
                    JsonNode n = OM.readTree(trimmed);
                    JsonNode val = n.get("value");
                    if (val != null && val.isTextual()) {
                        String inner = val.asText();
                        String innerTrim = inner.trim();
                        boolean looksDoc = (innerTrim.startsWith("{") && innerTrim.endsWith("}"))
                                || (innerTrim.startsWith("[") && innerTrim.endsWith("]"));
                        return new Unwrapped(inner, looksDoc);
                    }
                } catch (Exception ignore) {
                    // fall through
                }
            }
            return new Unwrapped(s, true); // assume it's the real doc JSON
        }

        // Maybe it's a JSON string literal of the document -> unescape via Jackson
        try {
            JsonNode n = OM.readTree(s);
            if (n.isTextual()) {
                String inner = n.asText();
                String innerTrim = inner.trim();
                boolean looksDoc = (innerTrim.startsWith("{") && innerTrim.endsWith("}"))
                        || (innerTrim.startsWith("[") && innerTrim.endsWith("]"));
                return new Unwrapped(inner, looksDoc);
            }
            // Fallback: if it's an object with "value": "<json>"
            JsonNode val = n.get("value");
            if (val != null && val.isTextual()) {
                String inner = val.asText();
                String innerTrim = inner.trim();
                boolean looksDoc = (innerTrim.startsWith("{") && innerTrim.endsWith("}"))
                        || (innerTrim.startsWith("[") && innerTrim.endsWith("]"));
                return new Unwrapped(inner, looksDoc);
            }
        } catch (Exception ignore) {
            // fall through
        }

        // Not a document (likely a primitive or a non-doc column)
        return new Unwrapped(s, false);
    }
}
