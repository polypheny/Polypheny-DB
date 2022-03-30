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

package org.polypheny.db.adapter.neo4j.rules.graph;

import javax.annotation.Nullable;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.graph.GraphUnwind;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

public class NeoGraphUnwind extends GraphUnwind implements NeoGraphAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     * @param index
     * @param alias
     */
    public NeoGraphUnwind( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, int index, @Nullable String alias ) {
        super( cluster, traits, input, index, alias );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {

    }

}
