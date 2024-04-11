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

package org.polypheny.db.algebra.core.lpg;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;


public class LpgTransformer extends Transformer {

    public final List<PolyType> operationOrder;
    public final Operation operation;


    /**
     * Creates a {@link LpgTransformer}.
     * Transforms the underlying {@link ModelTrait#GRAPH} node to a {@link ModelTrait#RELATIONAL} node.
     */
    public LpgTransformer( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, AlgDataType rowType, List<PolyType> operationOrder, Operation operation ) {
        super( cluster, inputs, null, traitSet.replace( ModelTrait.GRAPH ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, rowType, false );
        this.operationOrder = operationOrder;
        this.operation = operation;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + operationOrder.stream().map( PolyType::name ).collect( Collectors.joining( "$" ) ) + "$"
                + operation + "$"
                + getInputs().stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        AlgWriter terms = super.explainTerms( pw );
        int i = 0;
        for ( AlgNode input : getInputs() ) {
            terms.input( "input_" + i, input );
            i++;
        }
        return terms;
    }

}
