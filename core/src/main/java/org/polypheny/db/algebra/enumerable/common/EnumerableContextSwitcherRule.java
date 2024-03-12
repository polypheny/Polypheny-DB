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
import org.polypheny.db.algebra.logical.common.LogicalContextSwitcher;
import org.polypheny.db.plan.Convention;


public class EnumerableContextSwitcherRule extends ConverterRule {

    public EnumerableContextSwitcherRule() {
        super( LogicalContextSwitcher.class,
                operand -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER, "EnumerableContextSwitcherRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalContextSwitcher switcher = (LogicalContextSwitcher) alg;
        final AlgNode input = convert( switcher.getInput(), switcher.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) );

        return new EnumerableContextSwitcher( input );
    }

}
