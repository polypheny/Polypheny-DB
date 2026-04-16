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

import org.polypheny.db.schema.document.SchemaValidator.Violation;
import java.util.List;

/**
 * Public preflight scan result for ALTER SCHEMA.
 */
public final class SchemaAlterPreflightReport {

    public final boolean ok;
    public final long scanned;
    public final long failing;
    public final List<Violation> sample;


    public SchemaAlterPreflightReport( boolean ok, long scanned, long failing, List<Violation> sample ) {
        this.ok = ok;
        this.scanned = scanned;
        this.failing = failing;
        this.sample = sample;
    }


    public String compactSummary( int maxItems ) {
        if ( ok || sample == null || sample.isEmpty() ) {
            return "ok";
        }
        return new SchemaValidator.ValidationResult( false, sample ).compactSummary( maxItems );
    }

}
