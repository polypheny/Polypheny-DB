/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.document.planning;

import lombok.Getter;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

@Getter
public class ParquetDocFilter extends Filter implements EnumerableAlg {

    private final ParquetDocument entity;


    public ParquetDocFilter( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode condition, ParquetDocument entity ) {
        super( cluster, traitSet.replace( EnumerableConvention.INSTANCE ), input, condition );
        this.entity = entity;
    }


    @Override
    public ParquetDocFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new ParquetDocFilter( getCluster(), traitSet, input, condition, entity );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( ((double) entity.getParquetSource().getExportedColumns().size() + 2D) / ((double) entity.getTupleType().getFieldCount() + 2D) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        var scan = (ParquetDocScan) input;
        return scan.implement( implementor, pref );
    }

}
