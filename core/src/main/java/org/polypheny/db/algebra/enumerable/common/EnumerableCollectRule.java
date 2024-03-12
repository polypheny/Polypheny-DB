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

package org.polypheny.db.algebra.enumerable.common;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert an {@link org.polypheny.db.algebra.core.Collect} to an {@link EnumerableCollect}.
 */
public class EnumerableCollectRule extends ConverterRule {

    public EnumerableCollectRule() {
        super( Collect.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableCollectRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Collect collect = (Collect) alg;
        final AlgTraitSet traitSet = collect.getTraitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode input = collect.getInput();
        return new EnumerableCollect(
                alg.getCluster(),
                traitSet,
                convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) ),
                collect.getFieldName() );
    }

}

