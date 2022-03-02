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

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyRelationship;

@Getter
public class LogicalGraphPattern extends AbstractAlgNode {

    private final ImmutableList<PolyNode> nodes;
    private final ImmutableList<PolyRelationship> rels;


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public LogicalGraphPattern( AlgOptCluster cluster, AlgTraitSet traitSet, ImmutableList<PolyNode> nodes, ImmutableList<PolyRelationship> rels ) {
        super( cluster, traitSet );
        this.nodes = nodes;
        this.rels = rels;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$";
    }

}
