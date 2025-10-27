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
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.schema.document.SchemaValidator.Violation;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Preflight validator for schema alterations.
 * <p>
 * Streams existing documents from readable placements, validates them against a proposed
 * {@link DocumentSchema}, and returns a {@link SchemaAlterPreflightReport} containing counters
 * and a small representative sample of violations.
 */
public final class SchemaAlterPreflight {

    private SchemaAlterPreflight() {
    }


    public static SchemaAlterPreflightReport run(
            final Catalog catalog,
            final LogicalCollection coll,
            final DocumentSchema targetSchema,
            final Statement stmt ) {

        // Fast exit if the entity has no placements (nothing to scan)
        var snap = catalog.getSnapshot();
        var allocs = new ArrayList<>(snap.alloc().getFromLogical(coll.id));
        if (allocs.isEmpty()) {
            return new SchemaAlterPreflightReport(true, 0, 0, List.of());
        }

        final LongAdder scanned = new LongAdder();
        final LongAdder failing = new LongAdder();
        final List<Violation> sample = new ArrayList<>(16);

        // Build an MQL full-collection scan in the entity's namespace (DOCUMENT or RELATIONAL)
        final String mql = "db." + coll.name + ".find({})";

        QueryContext ctx = QueryContext.builder()
                .query(mql)
                .language(QueryLanguage.from("mql"))
                .origin("SchemaAlterPreflight")
                .statement(stmt)                      // reuse current statement/transaction
                .namespaceId(coll.namespaceId)        // << important: execute in the entity's namespace
                .build()
                .addTransaction(stmt.getTransaction());

        final List<ExecutedContext> execs = LanguageManager.getINSTANCE().anyQuery(ctx);
        for (ExecutedContext ex : execs) {
            if (ex.getException().isPresent()) {
                throw new RuntimeException("Document scan failed: " + ex.getException().get().getMessage(),
                        ex.getException().get());
            }

            ResultIterator ri = ex.getIterator();
            final int fetchSize = 10_000;

            try {
                while (true) {
                    // Batch API: each row is a List<PolyValue>; for find({}) it’s a single column = the whole document
                    List<List<PolyValue>> batch = ri.getNextBatch(fetchSize);
                    if (batch.isEmpty()) break;

                    for (List<PolyValue> row : batch) {
                        if (row == null || row.isEmpty()) continue;

                        String json;
                        try {
                            json = row.get(0).toJson(); // PolyValue → canonical JSON
                        } catch (Throwable t) {
                            String s = String.valueOf(row.get(0)).trim();
                            if (!(s.startsWith("{") || s.startsWith("["))) continue; // not a document row
                            json = s;
                        }

                        final BsonDocument doc;
                        try {
                            doc = BsonDocument.parse(json);
                        } catch (Exception badJson) {
                            failing.increment();
                            if (sample.size() < 16) {
                                sample.add(new Violation("$", "notValidJson", "Unparseable JSON row"));
                            }
                            continue;
                        }

                        scanned.increment();

                        BsonDocument docForValidation = doc; // default to original
                        if (doc.containsKey( DocumentType.DOCUMENT_ID)) {
                            // avoid mutating the row object
                            docForValidation = doc.clone();
                            docForValidation.remove(DocumentType.DOCUMENT_ID);
                        }
                        var res = SchemaValidator.validate(targetSchema, docForValidation);

                        if (!res.ok()) {
                            failing.increment();
                            if (sample.size() < 16) {
                                var vs = res.violations();
                                int room = 16 - sample.size();
                                sample.addAll(vs.subList(0, Math.min(vs.size(), room)));
                            }
                        }
                    }
                }
            } finally {
                try { ri.close(); } catch (Throwable ignore) {}
            }
        }

        return new SchemaAlterPreflightReport(
                /*ok*/ failing.sum() == 0L,
                /*scanned*/ scanned.sum(),
                /*failing*/ failing.sum(),
                /*sample*/ sample
        );
    }
}
