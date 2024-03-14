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

package org.polypheny.db.algebra.rules;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableInterpreter;
import org.polypheny.db.interpreter.Bindables.BindableScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.schema.types.ProjectableFilterableEntity;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Planner rule that converts
 * a {@link Filter}
 * on a {@link RelScan}
 * of a {@link FilterableEntity}
 * or a {@link ProjectableFilterableEntity}
 * to a {@link BindableScan}.
 *
 * The {@link #INTERPRETER} variant allows an intervening {@link EnumerableInterpreter}.
 *
 * @see ProjectScanRule
 */
public abstract class FilterScanRule extends AlgOptRule {

    /**
     * Rule that matches Filter on Scan.
     */
    public static final FilterScanRule INSTANCE =
            new FilterScanRule(
                    operand( Filter.class, operand( RelScan.class, null, FilterScanRule::test, none() ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "FilterScanRule" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    final Filter filter = call.alg( 0 );
                    final RelScan scan = call.alg( 1 );
                    apply( call, filter, scan );
                }
            };

    /**
     * Rule that matches Filter on EnumerableInterpreter on Scan.
     */
    public static final FilterScanRule INTERPRETER =
            new FilterScanRule(
                    operand(
                            Filter.class,
                            operand(
                                    EnumerableInterpreter.class,
                                    operand(
                                            RelScan.class,
                                            null, FilterScanRule::test, none() ) ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "FilterScanRule:interpreter" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    final Filter filter = call.alg( 0 );
                    final RelScan<?> scan = call.alg( 2 );
                    apply( call, filter, scan );
                }
            };


    /**
     * Creates a FilterScanRule.
     */
    protected FilterScanRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        super( operand, algBuilderFactory, description );
    }


    public static boolean test( RelScan<?> scan ) {
        // We can only push filters into a FilterableTable or ProjectableFilterableTable.
        return scan.entity.unwrap( FilterableEntity.class ).isPresent() || scan.entity.unwrap( ProjectableFilterableEntity.class ).isPresent();
    }


    protected void apply( AlgOptRuleCall call, Filter filter, RelScan<?> scan ) {
        final ImmutableList<Integer> projects;
        final ImmutableList.Builder<RexNode> filters = ImmutableList.builder();
        if ( scan instanceof BindableScan ) {
            final BindableScan bindableScan = (BindableScan) scan;
            filters.addAll( bindableScan.filters );
            projects = bindableScan.projects;
        } else {
            projects = scan.identity();
        }

        final Mapping mapping = Mappings.target( projects, scan.getEntity().getTupleType().getFieldCount() );
        filters.add( RexUtil.apply( mapping, filter.getCondition() ) );

        call.transformTo( BindableScan.create( scan.getCluster(), scan.getEntity(), filters.build(), projects ) );
    }

}
