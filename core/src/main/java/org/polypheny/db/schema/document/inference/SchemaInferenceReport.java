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

package org.polypheny.db.schema.document.inference;

import lombok.Builder;
import lombok.Value;
import org.polypheny.db.schema.document.SchemaMeta;
import java.util.List;

@Value
@Builder
public class SchemaInferenceReport {
    SchemaMeta candidate;               // proposed schema (normalized)
    long docsSeen;                      // total scanned (or estimated)
    long docsCovered;                   // would conform under STRICT
    List<String> exclusionsSample;      // sample _id’s that would be excluded under STRICT
    String schemaHash;                  // content hash of candidate
    // optional: suggested indexes from stats
    // path → statistics (presence %, types, min/max, etc.)
}

/*
SchemaInferer inferer = new SchemaInferer();
SchemaInferenceReport r = inferer.inferCollectionSchema(
        ns, coll,
        InferenceOptions.builder()
            .sampleSize(100_000)
            .requiredThreshold(0.95)
            .typeConfidence(0.98)
            .enumMaxCardinality(64)
            .detectDates(true)
            .includeIndexHints(true)
            .build(),
        CancellationToken.none());

upsertCollectionSchema(ns, coll, r.getCandidate(), EnforcementMode.WARN, ConflictPolicy.COERCE);
 */
