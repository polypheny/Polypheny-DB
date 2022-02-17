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

package org.polypheny.db.adapter.enumerable;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.LogicalStreamer;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;

public class EnumerableStreamerRule extends ConverterRule {

    public EnumerableStreamerRule() {
        super( LogicalStreamer.class,
                operand -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER, "EnumerableStreamer" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalStreamer streamer = (LogicalStreamer) alg;
        final AlgNode query = AlgOptRule.convert( streamer.getLeft(), streamer.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final AlgNode prepared = AlgOptRule.convert( streamer.getRight(), streamer.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        return EnumerableStreamer.create( query, prepared );
    }

}
