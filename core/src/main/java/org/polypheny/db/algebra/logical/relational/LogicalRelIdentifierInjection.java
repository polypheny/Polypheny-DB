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
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Relational expression that complements the results of its input expression with unique identifiers.
 */
@Getter
public class LogicalRelIdentifierInjection extends SingleAlg implements RelAlg {

    public final Entity entity;


    protected LogicalRelIdentifierInjection( Entity entity, AlgCluster cluster, AlgTraitSet traits, AlgNode input, AlgDataType rowType ) {
        super( cluster, traits, input );

        this.entity = entity;
        this.rowType = rowType;
    }


    public static LogicalRelIdentifierInjection create( Entity table, final AlgNode input, AlgDataType rowType ) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traits = input.getTraitSet();
        return new LogicalRelIdentifierInjection( table, cluster, traits, input, rowType );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                entity.hashCode()+ "&";
    }


    @Override
    public AlgNode unfoldView( @Nullable AlgNode parent, int index, AlgCluster cluster ) {
        return super.unfoldView( parent, index, cluster );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( getInput() );
        return planner.getCostFactory().makeCost( dRows, 0, 0 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalRelIdentifierInjection( entity, getCluster(), traitSet, sole( inputs ), getRowType() );
    }

}
