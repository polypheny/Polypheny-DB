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

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.graph.GraphProject;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class NeoGraphProject extends GraphProject implements NeoGraphAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     * @param projects
     */
    public NeoGraphProject( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects ) {
        super( cluster, traits, input, projects );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );

        if ( !implementor.isDml() ) {
            implementor.add( with_() );
        }
    }

}
