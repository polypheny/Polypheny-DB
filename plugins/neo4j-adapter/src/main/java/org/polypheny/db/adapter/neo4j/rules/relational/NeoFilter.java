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

package org.polypheny.db.adapter.neo4j.rules.relational;

import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

public class NeoFilter extends Filter implements NeoRelAlg {

    public NeoFilter( AlgCluster cluster, AlgTraitSet traits, AlgNode child, RexNode condition ) {
        super( cluster, traits, child, condition );
    }


    @Override
    public Filter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new NeoFilter( input.getCluster(), input.getTraitSet(), input, condition );
    }


    @Override
    public void implement( NeoRelationalImplementor implementor ) {
        implementor.visitChild( 0, getInput() );

        implementor.addFilter( this );
    }

}
