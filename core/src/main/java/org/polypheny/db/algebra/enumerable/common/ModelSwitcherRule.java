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
 */

package org.polypheny.db.algebra.enumerable.common;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;

public class ModelSwitcherRule extends AlgOptRule {

    public static ModelSwitcherRule MODEL_SWITCHER_RULE_DOC_REL = new ModelSwitcherRule( ModelTrait.DOCUMENT, ModelTrait.RELATIONAL );

    public static ModelSwitcherRule MODEL_SWITCHER_RULE_DOC_GRAPH = new ModelSwitcherRule( ModelTrait.DOCUMENT, ModelTrait.GRAPH );

    public static ModelSwitcherRule MODEL_SWITCHER_RULE_GRAPH_REL = new ModelSwitcherRule( ModelTrait.GRAPH, ModelTrait.RELATIONAL );

    public static ModelSwitcherRule MODEL_SWITCHER_RULE_GRAPH_DOC = new ModelSwitcherRule( ModelTrait.GRAPH, ModelTrait.DOCUMENT );

    public static ModelSwitcherRule MODEL_SWITCHER_RULE_REL_DOC = new ModelSwitcherRule( ModelTrait.RELATIONAL, ModelTrait.DOCUMENT );

    public static ModelSwitcherRule MODEL_SWITCHER_RULE_REL_GRAPH = new ModelSwitcherRule( ModelTrait.RELATIONAL, ModelTrait.GRAPH );


    public ModelSwitcherRule( ModelTrait in, ModelTrait out ) {
        super( operand( AlgNode.class, out, r -> false, any() ), "ModelSwitcherRule_" + in + "_" + out );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        AlgNode alg = call.alg( 0 );

        ModelTrait rootModel = call.getPlanner().getRoot().getTraitSet().getTrait( ModelTraitDef.INSTANCE );
        if ( call.getParents() == null && alg.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) == rootModel ) {
            // no reason to go up
            return;
        }

        AlgNode parent = call.getParents().get( 0 );

        LogicalTransformer transformer = LogicalTransformer.create(
                alg.getCluster(),
                List.of( alg ),
                alg.getTupleType().getFieldNames(),
                alg.getTraitSet().getTrait( ModelTraitDef.INSTANCE ),
                parent.getTraitSet().getTrait( ModelTraitDef.INSTANCE ),
                alg.getTupleType(),
                false );

        AlgNode node = parent.copy( parent.getTraitSet(), List.of( transformer ) );

        call.transformTo( node );
    }

}
