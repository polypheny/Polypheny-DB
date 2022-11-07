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

import org.polypheny.db.adapter.neo4j.NeoGraph;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.algebra.core.lpg.LpgScan;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.TranslatableGraph;
import org.polypheny.db.type.PolyType;

public class NeoLpgScan extends LpgScan implements NeoGraphAlg {


    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgScan}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traitSet Traits active for this node, including {@link org.polypheny.db.schema.ModelTrait#GRAPH}
     */
    public NeoLpgScan( AlgOptCluster cluster, AlgTraitSet traitSet, TranslatableGraph graph ) {
        super( cluster, traitSet, graph );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.setGraph( (NeoGraph) getGraph() );

        if ( rowType.getFieldList().size() == 1 ) {
            AlgDataTypeField field = rowType.getFieldList().get( 0 );
            if ( field.getType().getPolyType() == PolyType.GRAPH ) {
                implementor.setAll( true );
            }
        }

    }

}
