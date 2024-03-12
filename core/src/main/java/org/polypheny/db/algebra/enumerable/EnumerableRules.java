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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.enumerable;


import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.common.EnumerableBatchIteratorRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableCollectRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableConditionalExecuteFalseRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableConditionalExecuteRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableConditionalExecuteTrueRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableConstraintEnforcerRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableContextSwitcherRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableModifyCollectRule;
import org.polypheny.db.algebra.enumerable.document.EnumerableDocumentUnwindRule;
import org.polypheny.db.algebra.enumerable.document.EnumerableDocumentValuesRule;
import org.polypheny.db.algebra.enumerable.lpg.EnumerableLpgMatchRule;
import org.polypheny.db.algebra.enumerable.lpg.EnumerableLpgTransformerRule;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for the {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableRules {

    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    public static final boolean BRIDGE_METHODS = true;

    public static final AlgOptRule ENUMERABLE_JOIN_RULE = new EnumerableJoinRule();

    public static final AlgOptRule ENUMERABLE_MERGE_JOIN_RULE = new EnumerableMergeJoinRule();

    public static final AlgOptRule ENUMERABLE_SEMI_JOIN_RULE = new EnumerableSemiJoinRule();

    public static final AlgOptRule ENUMERABLE_CORRELATE_RULE = new EnumerableCorrelateRule( AlgFactories.LOGICAL_BUILDER );


    private EnumerableRules() {
    }


    public static final EnumerableConditionalExecuteRule ENUMERABLE_CONDITIONAL_EXECUTE_RULE = new EnumerableConditionalExecuteRule();

    public static final EnumerableConditionalExecuteTrueRule ENUMERABLE_CONDITIONAL_EXECUTE_TRUE_RULE = new EnumerableConditionalExecuteTrueRule();

    public static final EnumerableConditionalExecuteFalseRule ENUMERABLE_CONDITIONAL_EXECUTE_FALSE_RULE = new EnumerableConditionalExecuteFalseRule();

    public static final EnumerableStreamerRule ENUMERABLE_STREAMER_RULE = new EnumerableStreamerRule();

    public static final EnumerableContextSwitcherRule ENUMERABLE_CONTEXT_SWITCHER_RULE = new EnumerableContextSwitcherRule();

    public static final EnumerableBatchIteratorRule ENUMERABLE_BATCH_ITERATOR_RULE = new EnumerableBatchIteratorRule();

    public static final EnumerableConstraintEnforcerRule ENUMERABLE_CONSTRAINT_ENFORCER_RULE = new EnumerableConstraintEnforcerRule();

    public static final EnumerableProjectRule ENUMERABLE_PROJECT_RULE = new EnumerableProjectRule();

    public static final EnumerableFilterRule ENUMERABLE_FILTER_RULE = new EnumerableFilterRule();

    public static final EnumerableCalcRule ENUMERABLE_CALC_RULE = new EnumerableCalcRule();

    public static final EnumerableAggregateRule ENUMERABLE_AGGREGATE_RULE = new EnumerableAggregateRule();

    public static final EnumerableSortRule ENUMERABLE_SORT_RULE = new EnumerableSortRule();

    public static final EnumerableLimitRule ENUMERABLE_LIMIT_RULE = new EnumerableLimitRule();

    public static final EnumerableUnionRule ENUMERABLE_UNION_RULE = new EnumerableUnionRule();

    public static final EnumerableModifyCollectRule ENUMERABLE_MODIFY_COLLECT_RULE = new EnumerableModifyCollectRule();

    public static final EnumerableIntersectRule ENUMERABLE_INTERSECT_RULE = new EnumerableIntersectRule();

    public static final EnumerableMinusRule ENUMERABLE_MINUS_RULE = new EnumerableMinusRule();

    public static final EnumerableValuesRule ENUMERABLE_VALUES_RULE = new EnumerableValuesRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableDocumentValuesRule ENUMERABLE_DOCUMENT_VALUES_RULE = new EnumerableDocumentValuesRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableWindowRule ENUMERABLE_WINDOW_RULE = new EnumerableWindowRule();

    public static final EnumerableCollectRule ENUMERABLE_COLLECT_RULE = new EnumerableCollectRule();

    public static final EnumerableUncollectRule ENUMERABLE_UNCOLLECT_RULE = new EnumerableUncollectRule();

    public static final EnumerableFilterToCalcRule ENUMERABLE_FILTER_TO_CALC_RULE = new EnumerableFilterToCalcRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableProjectToCalcRule ENUMERABLE_PROJECT_TO_CALC_RULE = new EnumerableProjectToCalcRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableScanRule ENUMERABLE_TABLE_SCAN_RULE = new EnumerableScanRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableTableFunctionScanRule ENUMERABLE_TABLE_FUNCTION_SCAN_RULE = new EnumerableTableFunctionScanRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableTransformerRule ENUMERABLE_TRANSFORMER_RULE = new EnumerableTransformerRule();

    public static final EnumerableLpgMatchRule ENUMERABLE_GRAPH_MATCH_RULE = new EnumerableLpgMatchRule( AlgFactories.LOGICAL_BUILDER );

    public static final EnumerableLpgUnwindRule ENUMERABLE_UNWIND_RULE = new EnumerableLpgUnwindRule();

    public static final EnumerableDocumentUnwindRule ENUMERABLE_DOCUMENT_UNWIND_RULE = new EnumerableDocumentUnwindRule();

    public static final EnumerableLpgTransformerRule ENUMERABLE_GRAPH_TRANSFORMER_RULE = new EnumerableLpgTransformerRule();


}

