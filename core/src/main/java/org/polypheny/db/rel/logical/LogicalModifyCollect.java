/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.logical;


import java.util.List;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.core.ModifyCollect;
import org.polypheny.db.rel.core.Union;


/**
 * Sub-class of {@link Union} not targeted at any particular engine or calling convention.
 */
public final class LogicalModifyCollect extends ModifyCollect {

    /**
     * Creates a LogicalModifyCollect.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalModifyCollect( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
    }


    /**
     * Creates a LogicalUnion by parsing serialized output.
     */
    public LogicalModifyCollect( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalModifyCollect.
     */
    public static LogicalModifyCollect create( List<RelNode> inputs, boolean all ) {
        final RelOptCluster cluster = inputs.get( 0 ).getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalModifyCollect( cluster, traitSet, inputs, all );
    }


    @Override
    public LogicalModifyCollect copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalModifyCollect( getCluster(), traitSet, inputs, all );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}

