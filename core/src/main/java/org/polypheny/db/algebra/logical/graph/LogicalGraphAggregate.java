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
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.ImmutableBitSet;

public class LogicalGraphAggregate extends Aggregate implements GraphAlg {


    public LogicalGraphAggregate( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traits, child, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public Aggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return new LogicalGraphAggregate( input.getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + super.algCompareString();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.AGGREGATE;
    }

}
