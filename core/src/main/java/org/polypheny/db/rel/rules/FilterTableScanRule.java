/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.rules;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.adapter.enumerable.EnumerableInterpreter;
import org.polypheny.db.interpreter.Bindables;
import org.polypheny.db.interpreter.Bindables.BindableTableScan;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Planner rule that converts
 * a {@link Filter}
 * on a {@link TableScan}
 * of a {@link FilterableTable}
 * or a {@link ProjectableFilterableTable}
 * to a {@link BindableTableScan}.
 *
 * The {@link #INTERPRETER} variant allows an intervening {@link org.polypheny.db.adapter.enumerable.EnumerableInterpreter}.
 *
 * @see org.polypheny.db.rel.rules.ProjectTableScanRule
 */
public abstract class FilterTableScanRule extends RelOptRule {

    /**
     * Rule that matches Filter on TableScan.
     */
    public static final FilterTableScanRule INSTANCE =
            new FilterTableScanRule(
                    operand( Filter.class, operandJ( TableScan.class, null, FilterTableScanRule::test, none() ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "FilterTableScanRule" ) {
                @Override
                public void onMatch( RelOptRuleCall call ) {
                    final Filter filter = call.rel( 0 );
                    final TableScan scan = call.rel( 1 );
                    apply( call, filter, scan );
                }
            };

    /**
     * Rule that matches Filter on EnumerableInterpreter on TableScan.
     */
    public static final FilterTableScanRule INTERPRETER =
            new FilterTableScanRule(
                    operand(
                            Filter.class,
                            operand(
                                    EnumerableInterpreter.class,
                                    operandJ(
                                            TableScan.class,
                                            null, FilterTableScanRule::test, none() ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "FilterTableScanRule:interpreter" ) {
                @Override
                public void onMatch( RelOptRuleCall call ) {
                    final Filter filter = call.rel( 0 );
                    final TableScan scan = call.rel( 2 );
                    apply( call, filter, scan );
                }
            };


    /**
     * Creates a FilterTableScanRule.
     */
    protected FilterTableScanRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    public static boolean test( TableScan scan ) {
        // We can only push filters into a FilterableTable or ProjectableFilterableTable.
        final RelOptTable table = scan.getTable();
        return table.unwrap( FilterableTable.class ) != null || table.unwrap( ProjectableFilterableTable.class ) != null;
    }


    protected void apply( RelOptRuleCall call, Filter filter, TableScan scan ) {
        final ImmutableIntList projects;
        final ImmutableList.Builder<RexNode> filters = ImmutableList.builder();
        if ( scan instanceof BindableTableScan ) {
            final BindableTableScan bindableScan = (BindableTableScan) scan;
            filters.addAll( bindableScan.filters );
            projects = bindableScan.projects;
        } else {
            projects = scan.identity();
        }

        final Mapping mapping = Mappings.target( projects, scan.getTable().getRowType().getFieldCount() );
        filters.add( RexUtil.apply( mapping, filter.getCondition() ) );

        call.transformTo( Bindables.BindableTableScan.create( scan.getCluster(), scan.getTable(), filters.build(), projects ) );
    }
}
