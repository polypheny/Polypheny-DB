/*
 * Copyright 2019-2022 The Polypheny Project
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
 */

package org.polypheny.db.algebra.rules;

import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.graph.LogicalGraphProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTraitSet;

public class GraphToEnumerableRule extends AlgOptRule {

    public static GraphToEnumerableRule PROJECT_TO_ENUMERABLE = new GraphToEnumerableRule( operand( LogicalGraphProject.class, any() ), "GRAPH_PROJECT_TO_ENUMERABLE" );


    public GraphToEnumerableRule( AlgOptRuleOperand operand, String description ) {
        super( operand, AlgFactories.LOGICAL_BUILDER, description );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalGraphProject project = call.alg( 0 );
        AlgTraitSet out = project.getTraitSet().replace( EnumerableConvention.INSTANCE );
        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), out, project.getInput(), project.getProjects(), project.getRowType() );
        call.transformTo( enumerableProject );
    }

}
