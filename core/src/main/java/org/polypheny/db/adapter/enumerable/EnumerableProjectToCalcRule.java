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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Variant of {@link ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectToCalcRule} for {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableProjectToCalcRule extends RelOptRule {

    /**
     * Creates an EnumerableProjectToCalcRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public EnumerableProjectToCalcRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( EnumerableProject.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final EnumerableProject project = call.rel( 0 );
        final RelNode input = project.getInput();
        final RexProgram program = RexProgram.create( input.getRowType(), project.getProjects(), null, project.getRowType(), project.getCluster().getRexBuilder() );
        final EnumerableCalc calc = EnumerableCalc.create( input, program );
        call.transformTo( calc );
    }
}
