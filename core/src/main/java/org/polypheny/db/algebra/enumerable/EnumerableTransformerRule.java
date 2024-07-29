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

package org.polypheny.db.algebra.enumerable;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;


public class EnumerableTransformerRule extends ConverterRule {

    public EnumerableTransformerRule() {
        super( LogicalTransformer.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableTransformerRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalTransformer transformer = (LogicalTransformer) alg;
        List<AlgNode> inputs = transformer
                .getInputs()
                .stream()
                .map( i -> AlgOptRule.convert( i, i.getTraitSet()
                        .replace( EnumerableConvention.INSTANCE )
                        .replace( transformer.inModelTrait ) ) )
                .collect( Collectors.toList() );

        return new EnumerableTransformer(
                alg.getCluster(),
                inputs,
                transformer.names,
                transformer.getTraitSet().replace( EnumerableConvention.INSTANCE ),
                transformer.inModelTrait,
                transformer.outModelTrait,
                transformer.getTupleType(),
                transformer.isCrossModel );
    }

}
