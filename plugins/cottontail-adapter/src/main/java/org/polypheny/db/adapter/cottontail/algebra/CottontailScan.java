/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;


public class CottontailScan extends RelScan<CottontailEntity> implements CottontailAlg {

    protected final CottontailEntity cottontailTable;


    public CottontailScan( AlgOptCluster cluster, CottontailEntity cottontailTable, AlgTraitSet traitSet, CottontailConvention cottontailConvention ) {
        super( cluster, traitSet.replace( cottontailConvention ), cottontailTable );
        this.cottontailTable = cottontailTable;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new CottontailScan( getCluster(), this.cottontailTable, traitSet, (CottontailConvention) this.getConvention() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.8 );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
//        context.from = From.newBuilder().setEntity( this.cottontailTable.getTable() ).build();
        if ( context.queryType == null ) {
            context.cottontailTable = this.cottontailTable;
            context.schemaName = this.cottontailTable.getPhysicalSchemaName();
            context.tableName = this.cottontailTable.getPhysicalTableName();
            context.queryType = QueryType.SELECT;
        }
    }

}
