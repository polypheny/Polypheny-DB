/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.planning;

import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;

/**
 * Adapter between Polypheny’s rule engine and your own PatternMatcher abstraction
 * It does not contain Parquet planning logic itself. It only delegates.
 */
public class ParquetAlgOptRule extends AlgOptRule {

    private final PatternMatcher matcher;


    public ParquetAlgOptRule(PatternMatcher matcher ) {
        super( matcher.operand(), matcher.factory(), matcher.description() );
        this.matcher = matcher;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        matcher.onMatch( call );
    }

}
