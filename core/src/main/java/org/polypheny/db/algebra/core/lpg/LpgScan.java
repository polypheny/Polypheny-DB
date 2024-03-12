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

package org.polypheny.db.algebra.core.lpg;

import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;


public abstract class LpgScan<E extends Entity> extends Scan<E> implements LpgAlg {


    /**
     * Creates a {@link LpgScan}.
     * {@link ModelTrait#GRAPH} native node, which is able to relScan a LPG graph.
     */
    public LpgScan( AlgCluster cluster, AlgTraitSet traitSet, E graph ) {
        super( cluster, traitSet.replace( ModelTrait.GRAPH ), graph );
        this.rowType = GraphType.of();//new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "g", 0, cluster.getTypeFactory().createPolyType( PolyType.GRAPH ) ) ) );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + entity.id + "&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "id", entity.id )
                .item( "layer", entity.getLayer() );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.SCAN;
    }

}
