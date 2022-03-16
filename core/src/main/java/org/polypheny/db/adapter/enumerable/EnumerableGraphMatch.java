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

package org.polypheny.db.adapter.enumerable;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.GraphMatch;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class EnumerableGraphMatch extends GraphMatch implements EnumerableAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    protected EnumerableGraphMatch( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<RexNode> matches, List<String> names ) {
        super( cluster, traits, input, matches, names );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        return null;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + matches.hashCode() + "$" + names.hashCode();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.MATCH;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableGraphMatch( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), matches, names );
    }

}
