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

package org.polypheny.db.adapter.parquet.relational.optimization;

import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;
import java.util.function.Consumer;

public record PatternMatcher( Convention out, AlgBuilderFactory factory, AlgOptRuleOperand operand, String description, Consumer<AlgOptRuleCall> consumer ) {

    public void onMatch( AlgOptRuleCall call ) {
        consumer.accept( call );
    }

}

