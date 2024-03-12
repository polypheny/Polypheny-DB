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

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.trait.ModelTrait;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public abstract class DocumentFilter extends SingleAlg implements DocumentAlg {

    @NotNull
    public RexNode condition;


    /**
     * Creates a {@link DocumentFilter}.
     * {@link ModelTrait#DOCUMENT} native node of a filter.
     */
    protected DocumentFilter( AlgCluster cluster, AlgTraitSet traits, AlgNode input, @NotNull RexNode condition ) {
        super( cluster, traits, input );
        this.condition = condition;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + condition.hashCode() + "$"
                + input.algCompareString() + "&";
    }


    @Override
    public DocType getDocType() {
        return DocType.FILTER;
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        RexNode condition = shuttle.apply( this.condition );
        if ( this.condition == condition ) {
            return this;
        }
        return copy( traitSet, getInput(), condition );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, inputs.get( 0 ), condition );
    }


    protected abstract AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition );


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "condition", condition );
    }

}
