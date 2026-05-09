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

import java.util.List;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.type.PolyType;


final class ParquetRelScanRuleSupport {

    private ParquetRelScanRuleSupport() {
    }


    static ParquetRelScan findDirectRelScan( AlgNode alg ) {
        if ( alg == null ) {
            return null;
        }
        if ( alg instanceof ParquetRelScan relScan ) {
            return relScan;
        }
        if ( alg instanceof RelScan<?> relScan && relScan.getEntity().unwrap( ParquetRelTable.class ).isPresent() ) {
            ParquetRelTable table = relScan.getEntity().unwrapOrThrow( ParquetRelTable.class );
            return new ParquetRelScan( relScan.getCluster(), table, relScan.identity().stream().mapToInt( Integer::intValue ).toArray() );
        }
        if ( alg instanceof AlgSubset subset ) {
            ParquetRelScan relScan = findDirectRelScan( subset.getBest() );
            if ( relScan != null ) {
                return relScan;
            }
            relScan = findDirectRelScan( subset.getOriginal() );
            if ( relScan != null ) {
                return relScan;
            }
            for ( AlgNode candidate : subset.getAlgList() ) {
                relScan = findDirectRelScan( candidate );
                if ( relScan != null ) {
                    return relScan;
                }
            }
        }
        return null;
    }


    static List<PolyType> fieldTypes( AlgNode alg ) {
        return alg.getTupleType().getFields().stream()
                .map( field -> field.getType().getPolyType() )
                .toList();
    }

}
