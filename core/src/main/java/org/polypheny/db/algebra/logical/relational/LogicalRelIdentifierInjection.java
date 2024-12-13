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

package org.polypheny.db.algebra.logical.relational;

import java.util.List;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Streamer;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Relational expression that complements the results of its input expression with unique identifiers.
 */
@Getter
public class LogicalRelIdentifierInjection extends Streamer implements RelAlg {

    public final Entity entity;


    protected LogicalRelIdentifierInjection( Entity entity, AlgCluster cluster, AlgTraitSet traits, AlgNode provider, AlgNode collector ) {
        super( cluster, traits, provider, collector );
        this.entity = entity;
    }


    public static LogicalRelIdentifierInjection create( Entity entity, final AlgNode provider, AlgNode collector ) {
        final AlgCluster cluster = provider.getCluster();
        final AlgTraitSet traits = provider.getTraitSet();
        return new LogicalRelIdentifierInjection( entity, cluster, traits, provider, collector );
    }


    @Override
    public AlgNode unfoldView( @Nullable AlgNode parent, int index, AlgCluster cluster ) {
        return super.unfoldView( parent, index, cluster );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( getLeft() );
        return planner.getCostFactory().makeCost( dRows, 0, 0 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalRelIdentifierInjection( entity, getCluster(), traitSet, getLeft(), getRight() );
    }

}
