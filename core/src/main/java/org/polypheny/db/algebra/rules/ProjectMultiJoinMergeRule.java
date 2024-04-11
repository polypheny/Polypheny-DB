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


import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes {@link org.polypheny.db.algebra.core.Project} into a {@link MultiJoin}, creating a richer {@code MultiJoin}.
 *
 * @see org.polypheny.db.algebra.rules.FilterMultiJoinMergeRule
 */
public class ProjectMultiJoinMergeRule extends AlgOptRule {

    public static final ProjectMultiJoinMergeRule INSTANCE = new ProjectMultiJoinMergeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectMultiJoinMergeRule.
     */
    public ProjectMultiJoinMergeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalRelProject.class, operand( MultiJoin.class, any() ) ),
                algBuilderFactory,
                null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalRelProject project = call.alg( 0 );
        MultiJoin multiJoin = call.alg( 1 );

        // If all inputs have their projFields set, then projection information has already been pushed into each input
        boolean allSet = true;
        for ( int i = 0; i < multiJoin.getInputs().size(); i++ ) {
            if ( multiJoin.getProjFields().get( i ) == null ) {
                allSet = false;
                break;
            }
        }
        if ( allSet ) {
            return;
        }

        // Create a new MultiJoin that reflects the columns in the projection above the MultiJoin
        final AlgBuilder algBuilder = call.builder();
        MultiJoin newMultiJoin = AlgOptUtil.projectMultiJoin( multiJoin, project );
        algBuilder.push( newMultiJoin ).project( project.getProjects(), project.getTupleType().getFieldNames() );

        call.transformTo( algBuilder.build() );
    }

}

