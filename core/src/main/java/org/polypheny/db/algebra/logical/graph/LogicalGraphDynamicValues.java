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
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class LogicalGraphDynamicValues extends SingleAlg implements GraphAlg {

    private final List<RexNode> nodeOperations;
    private final List<RexNode> edgeOperations;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    public LogicalGraphDynamicValues(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            AlgNode input,
            List<RexNode> nodeOperations,
            List<RexNode> edgeOperations ) {
        super( cluster, traits, input );
        this.nodeOperations = nodeOperations;
        this.edgeOperations = edgeOperations;

    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() +
                "$" + (this.nodeOperations != null ? this.nodeOperations.hashCode() : "[]") +
                "$" + (this.edgeOperations != null ? this.edgeOperations.hashCode() : "[]");
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.VALUES_DYNAMIC;
    }

}
