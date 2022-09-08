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

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.ModelTrait;


public class EnumerableDocumentTransformerRule extends ConverterRule {

    public EnumerableDocumentTransformerRule() {
        super( ConverterImpl.class, r -> canApply( r ) && r.getTraitSet().allSimple(), ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, AlgFactories.LOGICAL_BUILDER, "EnumerableDocumentTransformer" );
    }


    private static boolean canApply( AlgNode r ) {
        return r.getTraitSet().contains( ModelTrait.RELATIONAL );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        AlgTraitSet out = alg.getTraitSet().replace( EnumerableConvention.INSTANCE ).replace( ModelTrait.DOCUMENT );
        AlgTraitSet inputOut = out.replace( ModelTrait.RELATIONAL );
        return new EnumerableDocumentTransformer(
                alg.getCluster(),
                List.of( convert( alg, inputOut ) ),
                out,
                alg.getRowType() );
    }

}
