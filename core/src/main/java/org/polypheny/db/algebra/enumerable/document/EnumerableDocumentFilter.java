/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable.document;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;

public class EnumerableDocumentFilter extends DocumentFilter implements EnumerableAlg {

    /**
     * Creates a {@link DocumentFilter}.
     * {@link ModelTrait#DOCUMENT} native node of a filter.
     *
     * @param cluster
     * @param traits
     * @param input
     * @param condition
     */
    protected EnumerableDocumentFilter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, RexNode condition ) {
        super( cluster, traits, input, condition );
    }


    @Override
    protected AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return null;
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        return null;
    }

}
