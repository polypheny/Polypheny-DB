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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;


public abstract class DocumentFilter extends SingleAlg implements DocumentAlg {

    public final RexNode condition;


    /**
     * Creates a {@link DocumentFilter}.
     * {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} native node of a filter.
     */
    protected DocumentFilter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, RexNode condition ) {
        super( cluster, traits, input );
        this.condition = condition;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + condition.hashCode() + "$" + input.algCompareString();
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


    protected abstract AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition );

}
