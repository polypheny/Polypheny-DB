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

@Value
@Builder
public class InferenceOptions {
    int sampleSize;                     // e.g., 50_000
    double requiredThreshold;           // e.g., 0.95 → mark field as "required" if present in ≥95% of docs
    double typeConfidence;              // e.g., 0.98 → promote dominant type only if ≥98%
    int enumMaxCardinality;             // e.g., ≤ 50 distinct values → suggest enum
    boolean includeIndexHints;          // emit suggested indexes in report
}

