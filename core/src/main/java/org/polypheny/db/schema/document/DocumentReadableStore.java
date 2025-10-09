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
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.transaction.Statement;
import java.util.function.Consumer;

/**
 * Optional SPI for document stores that can stream all documents of a collection.
 * Used to pre-validate ALTER SCHEMA tightenings.
 */
public interface DocumentReadableStore {

    /**
     * Stream all documents from the given allocation to the provided sink.
     * This must deliver top-level BSON documents as they are stored.
     */
    void scanCollection(
            Statement statement,
            LogicalCollection logical,
            AllocationCollection alloc,
            Consumer<BsonDocument> sink
    );
}
