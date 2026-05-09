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

package org.polypheny.db.adapter.parquet.relational.planning;

import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * Converts logical scans of Parquet physical tables into adapter scans.
 */
public class ParquetRelTableScanRule extends ConverterRule {

    public static final ParquetRelTableScanRule INSTANCE = new ParquetRelTableScanRule( AlgFactories.LOGICAL_BUILDER );


    public ParquetRelTableScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                LogicalRelScan.class,
                scan -> scan.getEntity().unwrap( ParquetRelTable.class ).isPresent(),
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                ParquetRelTableScanRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelScan scan = (LogicalRelScan) alg;
        ParquetRelTable table = scan.getEntity().unwrapOrThrow( ParquetRelTable.class );
        return new ParquetRelScan( scan.getCluster(), table, scan.identity().stream().mapToInt( Integer::intValue ).toArray() );
    }

}
