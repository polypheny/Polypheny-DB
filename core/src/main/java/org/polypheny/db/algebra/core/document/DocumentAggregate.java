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

package org.polypheny.db.algebra.core.document;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.ImmutableBitSet;


public class DocumentAggregate extends SingleAlg implements DocumentAlg {

    public final boolean indicator;
    public final List<AggregateCall> aggCalls;
    public final ImmutableBitSet groupSet;
    public final ImmutableList<ImmutableBitSet> groupSets;


    /**
     * Creates a {@link DocumentAggregate}.
     * {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} native node of an aggregate.
     */
    protected DocumentAggregate( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traits, child );
        this.indicator = indicator; // true is allowed, but discouraged
        this.aggCalls = ImmutableList.copyOf( aggCalls );
        this.groupSet = Objects.requireNonNull( groupSet );
        if ( groupSets == null ) {
            this.groupSets = ImmutableList.of( groupSet );
        } else {
            this.groupSets = ImmutableList.copyOf( groupSets );
            assert ImmutableBitSet.ORDERING.isStrictlyOrdered( groupSets ) : groupSets;
            for ( ImmutableBitSet set : groupSets ) {
                assert groupSet.contains( set );
            }
        }
        assert groupSet.length() <= child.getRowType().getFieldCount();
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (aggCalls != null ? aggCalls.stream().map( Objects::toString ).collect( Collectors.joining( " $ " ) ) : "") + "$" +
                (groupSet != null ? groupSet.toString() : "") + "$" +
                (groupSets != null ? groupSets.stream().map( Objects::toString ).collect( Collectors.joining( " $ " ) ) : "") + "$" +
                indicator + "&";
    }


    @Override
    public DocType getDocType() {
        return DocType.AGGREGATE;
    }

}
