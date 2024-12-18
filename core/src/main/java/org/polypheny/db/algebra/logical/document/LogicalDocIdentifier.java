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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Identifier;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

public class LogicalDocIdentifier extends Identifier implements DocumentAlg {
    protected LogicalDocIdentifier( Entity entitiy, AlgCluster cluster, AlgTraitSet traits, AlgNode input) {
        super(cluster, traits, entitiy, input);
    }

    public static LogicalDocIdentifier create(Entity document, final AlgNode input) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traits = input.getTraitSet();
        return new LogicalDocIdentifier( document, cluster, traits, input );
    }


    @Override
    public DocType getDocType() {
        // ToDo TH: is this correct?
        return DocType.VALUES;
    }

    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( getInput() );
        return planner.getCostFactory().makeCost( dRows, 0, 0 );
    }

    @Override
    public AlgNode copy(AlgTraitSet traitSete, List<AlgNode> inputs) {
        return new LogicalDocIdentifier(entity, getCluster(), traitSete, sole(inputs) );
    }


}
