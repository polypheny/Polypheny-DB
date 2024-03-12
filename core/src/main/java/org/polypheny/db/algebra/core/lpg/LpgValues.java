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

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;


@Getter
public abstract class LpgValues extends AbstractAlgNode implements LpgAlg {

    protected final ImmutableList<PolyNode> nodes;
    protected final ImmutableList<PolyEdge> edges;
    private final ImmutableList<ImmutableList<RexLiteral>> values;


    /**
     * Creates an {@link LpgValues}.
     * Which are either one or multiple nodes or edges, or literal values.
     */
    public LpgValues( AlgCluster cluster, AlgTraitSet traitSet, Collection<PolyNode> nodes, Collection<PolyEdge> edges, ImmutableList<ImmutableList<RexLiteral>> values, AlgDataType rowType ) {
        super( cluster, traitSet );
        this.nodes = ImmutableList.copyOf( nodes );
        this.edges = ImmutableList.copyOf( edges );
        this.values = values;
        this.rowType = rowType;
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.VALUES;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + nodes.hashCode() + "$"
                + edges.hashCode() + "$"
                + values.hashCode() + "&";
    }

}
