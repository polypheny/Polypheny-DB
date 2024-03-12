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
import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableInterpreter;
import org.polypheny.db.interpreter.Bindables.BindableScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.ProjectableFilterableEntity;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Planner rule that converts a {@link Project}
 * on a {@link RelScan}
 * of a {@link ProjectableFilterableEntity}
 * to a {@link BindableScan}.
 *
 * The {@link #INTERPRETER} variant allows an intervening {@link EnumerableInterpreter}.
 *
 * @see FilterScanRule
 */
public abstract class ProjectScanRule extends AlgOptRule {


    /**
     * Rule that matches Project on Scan.
     */
    public static final ProjectScanRule INSTANCE =
            new ProjectScanRule(
                    operand( Project.class, operand( RelScan.class, null, ProjectScanRule::test, none() ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "ProjectScanRule" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    final Project project = call.alg( 0 );
                    final RelScan<?> scan = call.alg( 1 );
                    apply( call, project, scan );
                }
            };

    /**
     * Rule that matches Project on EnumerableInterpreter on Scan.
     */
    public static final ProjectScanRule INTERPRETER =
            new ProjectScanRule(
                    operand( Project.class, operand( EnumerableInterpreter.class, operand( RelScan.class, null, ProjectScanRule::test, none() ) ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "ProjectScanRule:interpreter" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    final Project project = call.alg( 0 );
                    final RelScan<?> scan = call.alg( 2 );
                    apply( call, project, scan );
                }
            };


    /**
     * Creates a ProjectScanRule.
     */
    public ProjectScanRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        super( operand, algBuilderFactory, description );
    }


    protected static boolean test( RelScan<?> scan ) {
        // We can only push projects into a ProjectableFilterableTable.
        return scan.entity.unwrap( ProjectableFilterableEntity.class ).isPresent();
    }


    protected void apply( AlgOptRuleCall call, Project project, RelScan<?> scan ) {
        assert scan.entity.unwrap( ProjectableFilterableEntity.class ).isPresent();

        final TargetMapping mapping = project.getMapping();
        if ( mapping == null || Mappings.isIdentity( mapping ) ) {
            return;
        }

        final ImmutableList<Integer> projects;
        final ImmutableList<RexNode> filters;
        if ( scan instanceof BindableScan ) {
            final BindableScan bindableScan = (BindableScan) scan;
            filters = bindableScan.filters;
            projects = bindableScan.projects;
        } else {
            filters = ImmutableList.of();
            projects = scan.identity();
        }

        final List<Integer> projects2 = Mappings.apply( (Mapping) mapping, projects );
        call.transformTo( BindableScan.create( scan.getCluster(), scan.getEntity(), filters, projects2 ) );
    }

}

