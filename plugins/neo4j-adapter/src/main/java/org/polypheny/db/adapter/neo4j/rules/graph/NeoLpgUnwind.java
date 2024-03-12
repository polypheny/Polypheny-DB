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

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.unwind_;

import java.util.List;
import javax.annotation.Nullable;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgUnwind;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyString;

public class NeoLpgUnwind extends LpgUnwind implements NeoGraphAlg {

    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgUnwind}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link org.polypheny.db.catalog.logistic.DataModel#GRAPH}
     * @param input Input algebraic expression
     */
    public NeoLpgUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, int index, @Nullable String alias ) {
        super( cluster, traits, input, index, alias );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.8 );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() ); // todo fix subfield
        implementor.add( unwind_( as_( literal_( PolyString.of( implementor.getLast().getTupleType().getFieldNames().get( index ) ) ), literal_( PolyString.of( alias ) ) ) ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoLpgUnwind( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), index, alias );
    }

}
