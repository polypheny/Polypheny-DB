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

import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.graph.GraphSort;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class NeoGraphSort extends GraphSort implements NeoGraphAlg {

    public NeoGraphSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child, collation, offset, fetch );
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
        return new NeoGraphSort( input.getCluster(), traitSet, input, collation, offset, fetch );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {

    }

}
