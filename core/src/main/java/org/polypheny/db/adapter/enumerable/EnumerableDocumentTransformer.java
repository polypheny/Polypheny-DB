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
import org.polypheny.db.algebra.core.document.DocumentTransformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;


public class EnumerableDocumentTransformer extends DocumentTransformer implements EnumerableAlg {

    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public EnumerableDocumentTransformer( AlgOptCluster cluster, List<AlgNode> inputs, AlgTraitSet traitSet, AlgDataType rowType ) {
        super( cluster, inputs, traitSet, rowType );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        return implementor.visitChild( this, 0, (EnumerableAlg) getInput( 0 ), pref );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableDocumentTransformer( inputs.get( 0 ).getCluster(), inputs, traitSet, rowType );
    }

}
