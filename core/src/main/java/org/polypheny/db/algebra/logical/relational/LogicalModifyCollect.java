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

package org.polypheny.db.algebra.logical.relational;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.ModifyCollect;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Subclass of {@link ModifyCollect} not targeted at any particular engine or calling convention.
 */
public final class LogicalModifyCollect extends ModifyCollect {

    /**
     * Creates a LogicalModifyCollect.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalModifyCollect( AlgOptCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
    }


    /**
     * Creates a LogicalModifyCollect.
     */
    public static LogicalModifyCollect create( List<AlgNode> inputs, boolean all ) {
        final AlgOptCluster cluster = inputs.get( 0 ).getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalModifyCollect( cluster, traitSet, inputs, all );
    }


    @Override
    public LogicalModifyCollect copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalModifyCollect( getCluster(), traitSet, inputs, all );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}

