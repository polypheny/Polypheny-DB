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

package org.polypheny.db.algebra.logical.lpg;

import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.lpg.LpgAggregate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNameRef;


public class LogicalLpgAggregate extends LpgAggregate {


    public LogicalLpgAggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, @NotNull List<RexNameRef> groups, List<LaxAggregateCall> aggCalls, AlgDataType tupleType ) {
        super( cluster, traits, child, groups, aggCalls, tupleType );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgAggregate( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), groups, aggCalls, rowType );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
