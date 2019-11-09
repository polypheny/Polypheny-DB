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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ProjectableFilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mapping;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Planner rule that converts a {@link Project}
 * on a {@link TableScan}
 * of a {@link ProjectableFilterableTable}
 * to a {@link BindableTableScan}.
 *
 * The {@link #INTERPRETER} variant allows an intervening {@link EnumerableInterpreter}.
 *
 * @see FilterTableScanRule
 */
public abstract class ProjectTableScanRule extends RelOptRule {


    /**
     * Rule that matches Project on TableScan.
     */
    public static final ProjectTableScanRule INSTANCE =
            new ProjectTableScanRule(
                    operand( Project.class, operandJ( TableScan.class, null, ProjectTableScanRule::test, none() ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "ProjectScanRule" ) {
                @Override
                public void onMatch( RelOptRuleCall call ) {
                    final Project project = call.rel( 0 );
                    final TableScan scan = call.rel( 1 );
                    apply( call, project, scan );
                }
            };

    /**
     * Rule that matches Project on EnumerableInterpreter on TableScan.
     */
    public static final ProjectTableScanRule INTERPRETER =
            new ProjectTableScanRule(
                    operand( Project.class, operand( EnumerableInterpreter.class, operandJ( TableScan.class, null, ProjectTableScanRule::test, none() ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "ProjectScanRule:interpreter" ) {
                @Override
                public void onMatch( RelOptRuleCall call ) {
                    final Project project = call.rel( 0 );
                    final TableScan scan = call.rel( 2 );
                    apply( call, project, scan );
                }
            };


    /**
     * Creates a ProjectTableScanRule.
     */
    public ProjectTableScanRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    protected static boolean test( TableScan scan ) {
        // We can only push projects into a ProjectableFilterableTable.
        final RelOptTable table = scan.getTable();
        return table.unwrap( ProjectableFilterableTable.class ) != null;
    }


    protected void apply( RelOptRuleCall call, Project project, TableScan scan ) {
        final RelOptTable table = scan.getTable();
        assert table.unwrap( ProjectableFilterableTable.class ) != null;

        final TargetMapping mapping = project.getMapping();
        if ( mapping == null || Mappings.isIdentity( mapping ) ) {
            return;
        }

        final ImmutableIntList projects;
        final ImmutableList<RexNode> filters;
        if ( scan instanceof BindableTableScan ) {
            final BindableTableScan bindableScan = (BindableTableScan) scan;
            filters = bindableScan.filters;
            projects = bindableScan.projects;
        } else {
            filters = ImmutableList.of();
            projects = scan.identity();
        }

        final List<Integer> projects2 = Mappings.apply( (Mapping) mapping, projects );
        call.transformTo( Bindables.BindableTableScan.create( scan.getCluster(), scan.getTable(), filters, projects2 ) );
    }
}

