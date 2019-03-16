/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for the {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableRules {

    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    public static final boolean BRIDGE_METHODS = true;

    public static final RelOptRule ENUMERABLE_JOIN_RULE = new EnumerableJoinRule();

    public static final RelOptRule ENUMERABLE_MERGE_JOIN_RULE = new EnumerableMergeJoinRule();

    public static final RelOptRule ENUMERABLE_SEMI_JOIN_RULE = new EnumerableSemiJoinRule();

    public static final RelOptRule ENUMERABLE_CORRELATE_RULE = new EnumerableCorrelateRule( RelFactories.LOGICAL_BUILDER );


    private EnumerableRules() {
    }


    public static final EnumerableProjectRule ENUMERABLE_PROJECT_RULE = new EnumerableProjectRule();

    public static final EnumerableFilterRule ENUMERABLE_FILTER_RULE = new EnumerableFilterRule();

    public static final EnumerableCalcRule ENUMERABLE_CALC_RULE = new EnumerableCalcRule();

    public static final EnumerableAggregateRule ENUMERABLE_AGGREGATE_RULE = new EnumerableAggregateRule();

    public static final EnumerableSortRule ENUMERABLE_SORT_RULE = new EnumerableSortRule();

    public static final EnumerableLimitRule ENUMERABLE_LIMIT_RULE = new EnumerableLimitRule();

    public static final EnumerableUnionRule ENUMERABLE_UNION_RULE = new EnumerableUnionRule();

    public static final EnumerableIntersectRule ENUMERABLE_INTERSECT_RULE = new EnumerableIntersectRule();

    public static final EnumerableMinusRule ENUMERABLE_MINUS_RULE = new EnumerableMinusRule();

    public static final EnumerableTableModifyRule ENUMERABLE_TABLE_MODIFICATION_RULE = new EnumerableTableModifyRule( RelFactories.LOGICAL_BUILDER );

    public static final EnumerableValuesRule ENUMERABLE_VALUES_RULE = new EnumerableValuesRule( RelFactories.LOGICAL_BUILDER );

    public static final EnumerableWindowRule ENUMERABLE_WINDOW_RULE = new EnumerableWindowRule();

    public static final EnumerableCollectRule ENUMERABLE_COLLECT_RULE = new EnumerableCollectRule();

    public static final EnumerableUncollectRule ENUMERABLE_UNCOLLECT_RULE = new EnumerableUncollectRule();

    public static final EnumerableFilterToCalcRule ENUMERABLE_FILTER_TO_CALC_RULE = new EnumerableFilterToCalcRule( RelFactories.LOGICAL_BUILDER );

    public static final EnumerableProjectToCalcRule ENUMERABLE_PROJECT_TO_CALC_RULE = new EnumerableProjectToCalcRule( RelFactories.LOGICAL_BUILDER );

    public static final EnumerableTableScanRule ENUMERABLE_TABLE_SCAN_RULE = new EnumerableTableScanRule( RelFactories.LOGICAL_BUILDER );

    public static final EnumerableTableFunctionScanRule ENUMERABLE_TABLE_FUNCTION_SCAN_RULE = new EnumerableTableFunctionScanRule( RelFactories.LOGICAL_BUILDER );
}

