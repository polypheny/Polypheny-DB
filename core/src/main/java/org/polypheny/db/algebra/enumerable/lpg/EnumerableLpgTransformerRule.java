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

package org.polypheny.db.algebra.enumerable.lpg;

import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.lpg.LpgTransformer;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


public class EnumerableLpgTransformerRule extends ConverterRule {

    public EnumerableLpgTransformerRule() {
        super(
                LpgTransformer.class,
                r -> true,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER,
                EnumerableLpgTransformerRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LpgTransformer lpgTransformer = (LpgTransformer) alg;
        AlgTraitSet out = alg.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgTraitSet inputOut = out.replace( lpgTransformer.inModelTrait );
        return new EnumerableLpgTransformer(
                alg.getCluster(),
                out,
                alg.getInputs().stream().map( i -> convert( i, inputOut ) ).collect( Collectors.toList() ),
                alg.getTupleType(),
                lpgTransformer.operationOrder,
                lpgTransformer.operation );
    }

}
