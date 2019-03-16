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


import ch.unibas.dmi.dbis.polyphenydb.plan.MaterializedViewSubstitutionVisitor;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptMaterialization;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptMaterializations;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.Collections;
import java.util.List;


/**
 * Planner rule that converts
 * a {@link Filter}
 * on a {@link TableScan}
 * to a {@link Filter} on Materialized View
 */
public class MaterializedViewFilterScanRule extends RelOptRule {

    public static final MaterializedViewFilterScanRule INSTANCE = new MaterializedViewFilterScanRule( RelFactories.LOGICAL_BUILDER );

    private final HepProgram program = new HepProgramBuilder()
            .addRuleInstance( FilterProjectTransposeRule.INSTANCE )
            .addRuleInstance( ProjectMergeRule.INSTANCE )
            .build();


    /**
     * Creates a MaterializedViewFilterScanRule.
     */
    public MaterializedViewFilterScanRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( Filter.class, operand( TableScan.class, null, none() ) ), relBuilderFactory, "MaterializedViewFilterScanRule" );
    }


    public void onMatch( RelOptRuleCall call ) {
        final Filter filter = call.rel( 0 );
        final TableScan scan = call.rel( 1 );
        apply( call, filter, scan );
    }


    protected void apply( RelOptRuleCall call, Filter filter, TableScan scan ) {
        final RelOptPlanner planner = call.getPlanner();
        final List<RelOptMaterialization> materializations = planner.getMaterializations();
        if ( !materializations.isEmpty() ) {
            RelNode root = filter.copy( filter.getTraitSet(), Collections.singletonList( (RelNode) scan ) );
            List<RelOptMaterialization> applicableMaterializations = RelOptMaterializations.getApplicableMaterializations( root, materializations );
            for ( RelOptMaterialization materialization : applicableMaterializations ) {
                if ( RelOptUtil.areRowTypesEqual( scan.getRowType(), materialization.queryRel.getRowType(), false ) ) {
                    RelNode target = materialization.queryRel;
                    final HepPlanner hepPlanner = new HepPlanner( program, planner.getContext() );
                    hepPlanner.setRoot( target );
                    target = hepPlanner.findBestExp();
                    List<RelNode> subs = new MaterializedViewSubstitutionVisitor( target, root ).go( materialization.tableRel );
                    for ( RelNode s : subs ) {
                        call.transformTo( s );
                    }
                }
            }
        }
    }
}

