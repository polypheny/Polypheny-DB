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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;

public class NeoValues extends Values implements NeoRelAlg {

    public NeoValues( AlgCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    @Override
    public void implement( NeoRelationalImplementor implementor ) {
        if ( getTupleType().getFieldCount() == 1 && getTupleType().getFields().get( 0 ).getName().equals( "ZERO" ) ) {
            implementor.setPrepared( true );
        }

        implementor.addValues( tuples );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoValues( getCluster(), rowType, tuples, traitSet );
    }

}
