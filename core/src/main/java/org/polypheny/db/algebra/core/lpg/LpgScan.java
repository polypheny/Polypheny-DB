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

package org.polypheny.db.algebra.core.lpg;

import java.util.List;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;


public abstract class LpgScan<E extends CatalogEntity> extends Scan<E> implements LpgAlg {


    /**
     * Creates a {@link LpgScan}.
     * {@link org.polypheny.db.schema.ModelTrait#GRAPH} native node, which is able to scan a LPG graph.
     */
    public LpgScan( AlgOptCluster cluster, AlgTraitSet traitSet, E graph ) {
        super( cluster, traitSet, graph );
        this.rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "g", 0, cluster.getTypeFactory().createPolyType( PolyType.GRAPH ) ) ) );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + entity.id;
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.SCAN;
    }

}
