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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.document.DocumentAggregate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.util.ImmutableBitSet;


public class LogicalDocumentAggregate extends DocumentAggregate {

    /**
     * Subclass of {@link DocumentAggregate} not targeted at any particular engine or calling convention.
     */
    protected LogicalDocumentAggregate( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traits, child, indicator, groupSet, groupSets, aggCalls );
    }


    /**
     * Creates a LogicalAggregate.
     */
    public static LogicalDocumentAggregate create( final AlgNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return create_( input, false, groupSet, groupSets, aggCalls );
    }


    private static LogicalDocumentAggregate create_( final AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalDocumentAggregate( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return Aggregate.deriveRowType( getCluster().getTypeFactory(), getInput().getRowType(), indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentAggregate( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
