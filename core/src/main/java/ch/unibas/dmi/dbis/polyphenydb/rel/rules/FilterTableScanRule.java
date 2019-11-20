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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpreter;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Bindables;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Bindables.BindableTableScan;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.schema.FilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ProjectableFilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mapping;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import com.google.common.collect.ImmutableList;


/**
 * Planner rule that converts
 * a {@link Filter}
 * on a {@link TableScan}
 * of a {@link FilterableTable}
 * or a {@link ProjectableFilterableTable}
 * to a {@link BindableTableScan}.
 *
 * The {@link #INTERPRETER} variant allows an intervening {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpreter}.
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule
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
