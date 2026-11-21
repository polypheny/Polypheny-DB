/*
 * Copyright 2019-2026 The Polypheny Project
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
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.Triple;

public final class LogicalLpgJoin extends Join implements LpgAlg {

    private LogicalLpgJoin( AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variables, JoinAlgType type ) {
        super( left.getCluster(), traits.replace( ModelTrait.GRAPH ), left, right, condition, variables, type );
    }


    public static LogicalLpgJoin create( AlgNode left, AlgNode right, RexNode condition, JoinAlgType type ) {
        return new LogicalLpgJoin( left.getTraitSet(), left, right, condition, Set.of(), type );
    }


    public static LogicalLpgJoin create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        Triple<RexNode, Set<CorrelationId>, JoinAlgType> values = extractArgs( args );
        AlgNode left = children.get( 0 );
        return new LogicalLpgJoin( left.getTraitSet(), left, children.get( 1 ), values.left, values.middle, values.right );
    }


    @Override
    public LogicalLpgJoin copy( AlgTraitSet traits, RexNode condition, AlgNode left, AlgNode right, JoinAlgType type, boolean semiJoinDone ) {
        return new LogicalLpgJoin( traits, left, right, condition, variablesSet, type );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.JOIN;
    }

}
