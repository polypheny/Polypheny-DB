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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.common.LogicalBatchIterator;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;


public class EnumerableBatchIteratorRule extends ConverterRule {

    public EnumerableBatchIteratorRule() {
        super( LogicalBatchIterator.class,
                operand -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER, "EnumerableBatchIterator" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalBatchIterator iterator = (LogicalBatchIterator) alg;
        final AlgNode input = AlgOptRule.convert( iterator.getInput(), iterator.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        return EnumerableBatchIterator.create( input );
    }

}
