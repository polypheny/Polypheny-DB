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

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.where_;

import java.util.HashMap;
import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.Translator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgFilter;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;

public class NeoLpgFilter extends LpgFilter implements NeoGraphAlg {


    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgFilter}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link DataModel#GRAPH}
     * @param input Input algebraic expression
     */
    public NeoLpgFilter( AlgCluster cluster, AlgTraitSet traits, AlgNode input, RexNode condition ) {
        super( cluster, traits, input, condition );
    }


    @Override
    protected AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new NeoLpgFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        Translator translator = new Translator( getTupleType(), implementor.getLast().getTupleType(), new HashMap<>(), null, implementor.getGraph().mappingLabel, false );
        implementor.add( where_( list_( List.of( literal_( PolyString.of( getCondition().accept( translator ) ) ) ) ) ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoLpgFilter( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), getCondition() );
    }

}
