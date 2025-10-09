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
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.schema.document.SchemaValidator.ValidationResult;
import org.polypheny.db.schema.document.SchemaValidator.Violation;
import org.polypheny.db.transaction.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class SchemaAlterPreflight {

    private SchemaAlterPreflight() {}

    /**
     * Validate all existing documents in one placement of the collection against the proposed schema.
     * Returns a report with counts and a small sample of violations.
     */
    public static SchemaAlterPreflightReport run(
            Catalog catalog,
            LogicalCollection coll,
            DocumentSchema schema,
            Statement stmt) {

        Snapshot snapshot = catalog.getSnapshot();

        List<AllocationEntity> allocations = snapshot.alloc().getFromLogical(coll.id);
        if (allocations.isEmpty()) {
            return new SchemaAlterPreflightReport(true, 0, 0, List.of());
        }

        AllocationEntity ae = allocations.get(0);
        AllocationCollection alloc = ae.unwrapOrThrow(AllocationCollection.class);

        DataStore<?> store = AdapterManager.getInstance().getStore(ae.adapterId).orElseThrow();
        if (!(store instanceof DocumentReadableStore drs)) {
            throw new GenericRuntimeException(
                    "ALTER SCHEMA requires a full scan, but store %s (adapterId=%d) does not implement DocumentReadableStore.",
                    store.getUniqueName(), ae.adapterId
            );
        }

        final long[] scanned = {0};
        final long[] failing = {0};
        final List<Violation> sample = new ArrayList<>(16);

        Consumer<BsonDocument> sink = d -> {
            scanned[0]++;
            ValidationResult vr = SchemaValidator.validate(schema, d);
            if (!vr.ok()) {
                failing[0]++;
                if (sample.size() < 16) sample.addAll(vr.violations());
            }
        };

        drs.scanCollection(stmt, coll, alloc, sink);
        return new SchemaAlterPreflightReport(failing[0] == 0, scanned[0], failing[0], sample);
    }
}
