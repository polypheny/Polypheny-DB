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
import java.util.Optional;
import java.util.function.DoubleSupplier;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Identifier;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

@Getter
public class LogicalRelIdentifier extends Identifier implements RelAlg {

    protected LogicalRelIdentifier( long version, Entity entity, AlgCluster cluster, AlgTraitSet traits, AlgNode input, AlgDataType rowType ) {
        super( cluster, traits, version, entity, input );
        this.rowType = rowType;
    }


    public static LogicalRelIdentifier create(long version, Entity table, final AlgNode input, AlgDataType rowType ) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traits = input.getTraitSet();
        return new LogicalRelIdentifier( version, table, cluster, traits, input, rowType );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        Optional<Double> dRows = mq.getTupleCount( getInput() );
        if ( dRows.isEmpty() ) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        return planner.getCostFactory().makeCost( dRows.get(), 0, 0 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalRelIdentifier(version, entity, getCluster(), traitSet, sole( inputs ), getRowType() );
    }

}
