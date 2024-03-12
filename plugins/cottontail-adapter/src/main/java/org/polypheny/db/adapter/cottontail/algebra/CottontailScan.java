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

package org.polypheny.db.adapter.cottontail.algebra;


import java.util.List;
import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.CottontailEntity;
import org.polypheny.db.adapter.cottontail.algebra.CottontailAlg.CottontailImplementContext.QueryType;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;


public class CottontailScan extends RelScan<CottontailEntity> implements CottontailAlg {


    public CottontailScan( AlgCluster cluster, CottontailEntity cottontailTable, CottontailConvention cottontailConvention ) {
        super( cluster, cluster.traitSetOf( cottontailConvention ).replace( ModelTrait.RELATIONAL ), cottontailTable );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new CottontailScan( getCluster(), entity, (CottontailConvention) getConvention() );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$"
                + entity.id + "$"
                + entity.getLayer() + "&";
    }

    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        if ( context.queryType == null ) {
            context.table = this.entity;
            context.schemaName = this.entity.getPhysicalSchemaName();
            context.tableName = this.entity.getPhysicalTableName();
            context.queryType = QueryType.SELECT;
        }
    }

}
