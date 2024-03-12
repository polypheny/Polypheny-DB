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
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert an {@link LogicalRelUnion} to an {@link EnumerableUnion}.
 */
public class EnumerableModifyCollectRule extends ConverterRule {

    public EnumerableModifyCollectRule() {
        super( LogicalModifyCollect.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableModifyCollectRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalModifyCollect union = (LogicalModifyCollect) alg;
        final EnumerableConvention out = EnumerableConvention.INSTANCE;
        final AlgTraitSet traitSet = union.getTraitSet().replace( out );
        return new EnumerableModifyCollect( alg.getCluster(), traitSet, convertList( union.getInputs(), out ), true );
    }

}

