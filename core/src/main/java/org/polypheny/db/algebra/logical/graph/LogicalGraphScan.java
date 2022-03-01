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

package org.polypheny.db.algebra.logical.graph;


import javax.annotation.Nullable;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

public class LogicalGraphScan extends AbstractAlgNode implements GraphAlg, RelationalTransformable {

    private final String label;
    private final long namespaceId;


    public LogicalGraphScan( AlgOptCluster cluster, AlgTraitSet traitSet, long namespaceId, @Nullable String label ) {
        super( cluster, traitSet );
        this.namespaceId = namespaceId;
        this.label = label;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + namespaceId + "$" + label;
    }


    @Override
    public AlgNode getRelationalEquivalent() {
        return null;
    }

}
