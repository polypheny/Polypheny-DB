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

package org.polypheny.db.adapter.parquet.relational.filter;

import java.util.function.Function;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetFilterEvaluator;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


public class ParquetSourceFilePartitionFilterEvaluator extends ParquetFilterEvaluator<ParquetSourceFile, PolyValue> {

    private final Function<ParquetAdapterFilter<PolyValue>, ParquetColumnBinding> selector;


    public ParquetSourceFilePartitionFilterEvaluator( Function<ParquetAdapterFilter<PolyValue>, ParquetColumnBinding> selector ) {
        this.selector = selector;
    }


    @Override
    protected Boolean evaluateLeaf( ParquetSourceFile sourceFile, ParquetAdapterFilter<PolyValue> filter ) {
        ParquetColumnBinding columnBinding = selector.apply( filter );
        if ( columnBinding == null || columnBinding.role() != ParquetColumnRole.PARTITION ) {
            return null;
        }
        String partitionValue = sourceFile.partitionValues().get( columnBinding.columnName() );
        if ( partitionValue == null ) {
            return matchesValue( PolyNull.NULL, filter.operator(), filter.value() );
        }
        return matchesValue( PolyString.of( partitionValue ), filter.operator(), filter.value() );
    }

}
