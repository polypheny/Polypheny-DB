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
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.IdentifierCollector;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.transaction.Transaction;

public class LogicalLpgIdCollector extends IdentifierCollector implements LpgAlg {

    protected LogicalLpgIdCollector( AlgCluster cluster, AlgTraitSet traits, Transaction transaction, Entity entity,  AlgNode input ) {
        super( cluster, traits, transaction, entity,  input );
    }


    public static LogicalLpgIdCollector create(  final AlgNode input, Transaction transaction, Entity entity ) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traits = input.getTraitSet();
        return new LogicalLpgIdCollector( cluster, traits, transaction, entity, input );
    }


    @Override
    public NodeType getNodeType() {
        // ToDo: Is this the proper type here?
        return NodeType.VALUES;
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
        return new LogicalLpgIdCollector( getCluster(), traitSet, transaction, entity, sole( inputs ) );
    }

}
