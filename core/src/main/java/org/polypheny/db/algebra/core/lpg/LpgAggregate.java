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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.schema.trait.ModelTrait;


public abstract class LpgAggregate extends SingleAlg implements LpgAlg {


    @NotNull
    public final List<RexNameRef> groups;
    @NotNull
    public final List<LaxAggregateCall> aggCalls;


    /**
     * Creates a {@link LpgAggregate}.
     * {@link ModelTrait#GRAPH} native node of a aggregate.
     */
    protected LpgAggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, @NotNull List<RexNameRef> groups, @NotNull List<LaxAggregateCall> aggCalls, AlgDataType tupleType ) {
        super( cluster, traits, child );
        this.groups = groups;
        this.aggCalls = aggCalls;
        this.rowType = tupleType;
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                groups.hashCode() + "$" +
                aggCalls.stream().map( Objects::toString ).collect( Collectors.joining( " $ " ) ) + "&";
    }


    @Override
    protected AlgDataType deriveRowType() {
        AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        builder.add( "_id", null, DocumentType.ofDoc() );

        for ( LaxAggregateCall aggCall : aggCalls ) {
            builder.add( aggCall.name, null, DocumentType.ofDoc() );
        }

        return builder.build();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.AGGREGATE;
    }

}
