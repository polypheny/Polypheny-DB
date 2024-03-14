/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.algebra.rules;

import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.rules.ReduceExpressionsRule.CalcReduceExpressionsRule;
import org.polypheny.db.algebra.rules.ReduceExpressionsRule.FilterReduceExpressionsRule;
import org.polypheny.db.algebra.rules.ReduceExpressionsRule.JoinReduceExpressionsRule;
import org.polypheny.db.algebra.rules.ReduceExpressionsRule.ProjectReduceExpressionsRule;

public interface ReduceExpressionsRules {

    /**
     * Singleton rule that reduces constants inside a {@link LogicalRelFilter}.
     */
    ReduceExpressionsRule FILTER_INSTANCE = new FilterReduceExpressionsRule( LogicalRelFilter.class, true, AlgFactories.LOGICAL_BUILDER );

    /**
     * Singleton rule that reduces constants inside a {@link LogicalRelProject}.
     */
    ReduceExpressionsRule PROJECT_INSTANCE = new ProjectReduceExpressionsRule( LogicalRelProject.class, true, AlgFactories.LOGICAL_BUILDER );

    /**
     * Singleton rule that reduces constants inside a {@link Join}.
     */
    ReduceExpressionsRule JOIN_INSTANCE = new JoinReduceExpressionsRule( Join.class, true, AlgFactories.LOGICAL_BUILDER );

    /**
     * Singleton rule that reduces constants inside a {@link LogicalCalc}.
     */
    ReduceExpressionsRule CALC_INSTANCE = new CalcReduceExpressionsRule( LogicalCalc.class, true, AlgFactories.LOGICAL_BUILDER );

}
