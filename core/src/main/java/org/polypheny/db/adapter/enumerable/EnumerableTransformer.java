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
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Transformer;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Getter
public class EnumerableTransformer extends Transformer implements EnumerableAlg {


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     * @param inputs
     */
    public EnumerableTransformer( AlgOptCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs ) {
        super( cluster, traitSet, inputs );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        return null;
    }

}
