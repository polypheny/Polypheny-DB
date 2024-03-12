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

package org.polypheny.db.algebra.logical.lpg;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


public class LogicalLpgUnion extends Union {

    /**
     * Subclass of {@link Union} not targeted at any particular engine or calling convention.
     */
    protected LogicalLpgUnion( AlgCluster cluster, AlgTraitSet traits, List<AlgNode> inputs, boolean all ) {
        super( cluster, traits, inputs, all );
    }


    @Override
    public SetOp copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        return new LogicalLpgUnion( inputs.get( 0 ).getCluster(), traitSet, inputs, all );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
