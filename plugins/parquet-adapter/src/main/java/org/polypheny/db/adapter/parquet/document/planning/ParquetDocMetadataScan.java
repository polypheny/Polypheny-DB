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

import java.util.List;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Structural metadata-access input for document metadata aggregates.
 * Runtime execution is owned by the aggregate node, so no row scan cost is added.
 */
public class ParquetDocMetadataScan extends ParquetDocScan {

    public ParquetDocMetadataScan( ParquetDocScan scan ) {
        this( scan.getCluster(), scan.getEntity(), scan.getFilters() );
    }


    private ParquetDocMetadataScan( AlgCluster cluster, ParquetDocument entity, List<ParquetAdapterFilter<PolyValue>> filters ) {
        super( cluster, entity, filters );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new ParquetDocMetadataScan( getCluster(), entity, getFilters() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return planner.getCostFactory().makeZeroCost();
    }

}
