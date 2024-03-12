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

package org.polypheny.db.algebra.core.common;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;


public class ContextSwitcher extends SingleAlg {

    /**
     * Creates a {@link ContextSwitcher}.
     *
     * The default {@link org.polypheny.db.adapter.DataContext} only handles symmetric tuples of values. This poses a problem
     * if multiple nodes of an algebra tree are replaced during execution, which hold uneven amounts of values. The
     * {@link ContextSwitcher} separates the underlying algebra branch into its own capsuled instance, with its
     * own {@link org.polypheny.db.adapter.DataContext}.
     *
     * @param input Input relational expression, which requires its own {@link org.polypheny.db.adapter.DataContext}
     */
    protected ContextSwitcher( AlgNode input ) {
        super( input.getCluster(), input.getTraitSet(), input );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + input.algCompareString() + "&";
    }

}
