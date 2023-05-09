/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable.document;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexProgram;

public class DocumentProjectToCalcRule extends AlgOptRule {

    public static final DocumentProjectToCalcRule INSTANCE = new DocumentProjectToCalcRule();


    public DocumentProjectToCalcRule() {
        super( operand( LogicalDocumentProject.class, any() ), AlgFactories.LOGICAL_BUILDER, "DocumentToCalcRule_LogicalDocumentProject" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalDocumentProject project = call.alg( 0 );
        final AlgNode input = project.getInput();
        final RexProgram program = RexProgram.create( input.getRowType(), project.projects, null, project.getRowType(), project.getCluster().getRexBuilder() );
        final EnumerableCalc calc = EnumerableCalc.create( convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) ), program );
        call.transformTo( calc );
    }

}
