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
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.lpg.LpgScan;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;


public class LogicalLpgScan extends LpgScan<Entity> implements RelationalTransformable {

    /**
     * Subclass of {@link LpgScan} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgScan( AlgCluster cluster, AlgTraitSet traitSet, Entity graph, AlgDataType rowType ) {
        super( cluster, traitSet.replace( ModelTrait.GRAPH ), graph );
        this.rowType = rowType;
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> inputs, List<Entity> entities, Snapshot snapshot ) {
        assert !entities.isEmpty();
        AlgTraitSet out = getTraitSet().replace( ModelTrait.RELATIONAL );
        LogicalRelScan nodes = new LogicalRelScan( getCluster(), out, entities.get( 0 ) );
        LogicalRelScan nodesProperty = new LogicalRelScan( getCluster(), out, entities.get( 1 ) );

        RexBuilder builder = getCluster().getRexBuilder();

        RexNode nodeCondition = builder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                builder.makeInputRef( nodes.getTupleType().getFields().get( 0 ).getType(), 0 ),
                builder.makeInputRef( nodesProperty.getTupleType().getFields().get( 0 ).getType(), nodes.getTupleType().getFields().size() ) );

        LogicalRelJoin nodeJoin = new LogicalRelJoin( getCluster(), out, nodes, nodesProperty, nodeCondition, Set.of(), JoinAlgType.LEFT, false );

        if ( entities.size() == 2 ) {
            return List.of( nodeJoin );
        }

        LogicalRelScan edges = new LogicalRelScan( getCluster(), out, entities.get( 2 ) );
        LogicalRelScan edgesProperty = new LogicalRelScan( getCluster(), out, entities.get( 3 ) );

        RexNode edgeCondition = builder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                builder.makeInputRef( edges.getTupleType().getFields().get( 0 ).getType(), 0 ),
                builder.makeInputRef( edgesProperty.getTupleType().getFields().get( 0 ).getType(), edges.getTupleType().getFields().size() ) );

        LogicalRelJoin edgeJoin = new LogicalRelJoin( getCluster(), out, edges, edgesProperty, edgeCondition, Set.of(), JoinAlgType.LEFT, false );

        return List.of( nodeJoin, edgeJoin );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgScan( inputs.get( 0 ).getCluster(), traitSet, entity, rowType );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
