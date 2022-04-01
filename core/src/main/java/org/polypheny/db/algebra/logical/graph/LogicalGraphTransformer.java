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

package org.polypheny.db.algebra.logical.graph;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.GraphTransformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;

public class LogicalGraphTransformer extends GraphTransformer {

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param inputs
     * @param rowType
     * @param operationOrder
     */
    public LogicalGraphTransformer( AlgOptCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, AlgDataType rowType, List<PolyType> operationOrder ) {
        super( cluster, traitSet, inputs, rowType, operationOrder );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalGraphTransformer( inputs.get( 0 ).getCluster(), traitSet, inputs, rowType, operationOrder );
    }

}
