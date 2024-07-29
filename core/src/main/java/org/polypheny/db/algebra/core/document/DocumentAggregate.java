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

package org.polypheny.db.algebra.core.document;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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


public class DocumentAggregate extends SingleAlg implements DocumentAlg {

    @NotNull
    public final List<LaxAggregateCall> aggCalls;

    @Nullable // null means "group by all fields in the input tuple"
    private final RexNameRef group;


    /**
     * Creates a {@link DocumentAggregate}.
     * {@link ModelTrait#DOCUMENT} native node of an aggregate.
     */
    protected DocumentAggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, @Nullable RexNameRef group, List<LaxAggregateCall> aggCalls ) {
        super( cluster, traits, child );
        this.group = group;
        this.aggCalls = ImmutableList.copyOf( aggCalls );
        this.rowType = DocumentType.ofDoc();
    }


    public Optional<RexNameRef> getGroup() {
        return Optional.ofNullable( group );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (group != null ? group.hashCode() : "") + "$" +
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
    public DocType getDocType() {
        return DocType.AGGREGATE;
    }

}
