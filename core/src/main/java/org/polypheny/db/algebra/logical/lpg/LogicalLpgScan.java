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

package org.polypheny.db.algebra.logical.lpg;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.lpg.LpgScan;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.TranslatableGraph;


public class LogicalLpgScan extends LpgScan implements RelationalTransformable {

    /**
     * Subclass of {@link LpgScan} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgScan( AlgOptCluster cluster, AlgTraitSet traitSet, TranslatableGraph graph, AlgDataType rowType ) {
        super( cluster, traitSet, graph );
        this.rowType = rowType;
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> inputs, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        assert !entities.isEmpty();
        AlgTraitSet out = getTraitSet().replace( ModelTrait.RELATIONAL );
        LogicalScan nodes = new LogicalScan( getCluster(), out, entities.get( 0 ) );
        LogicalScan nodesProperty = new LogicalScan( getCluster(), out, entities.get( 1 ) );

        RexBuilder builder = getCluster().getRexBuilder();

        RexNode nodeCondition = builder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                builder.makeInputRef( nodes.getRowType().getFieldList().get( 0 ).getType(), 0 ),
                builder.makeInputRef( nodesProperty.getRowType().getFieldList().get( 0 ).getType(), nodes.getRowType().getFieldList().size() ) );

        LogicalJoin nodeJoin = new LogicalJoin( getCluster(), out, nodes, nodesProperty, nodeCondition, Set.of(), JoinAlgType.LEFT, false, ImmutableList.of() );

        if ( entities.size() == 2 ) {
            return List.of( nodeJoin );
        }

        LogicalScan edges = new LogicalScan( getCluster(), out, entities.get( 2 ) );
        LogicalScan edgesProperty = new LogicalScan( getCluster(), out, entities.get( 3 ) );

        RexNode edgeCondition = builder.makeCall(
                OperatorRegistry.get( OperatorName.EQUALS ),
                builder.makeInputRef( edges.getRowType().getFieldList().get( 0 ).getType(), 0 ),
                builder.makeInputRef( edgesProperty.getRowType().getFieldList().get( 0 ).getType(), edges.getRowType().getFieldList().size() ) );

        LogicalJoin edgeJoin = new LogicalJoin( getCluster(), out, edges, edgesProperty, edgeCondition, Set.of(), JoinAlgType.LEFT, false, ImmutableList.of() );

        return List.of( nodeJoin, edgeJoin );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgScan( inputs.get( 0 ).getCluster(), traitSet, graph, rowType );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
