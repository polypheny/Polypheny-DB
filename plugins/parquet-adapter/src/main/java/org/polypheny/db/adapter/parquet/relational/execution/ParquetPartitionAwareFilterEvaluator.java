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

package org.polypheny.db.adapter.parquet.relational.execution;

import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetValueExtractor;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetGroupFilterEvaluator;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Evaluates filter based on partition values.
 */
class ParquetPartitionAwareFilterEvaluator extends ParquetGroupFilterEvaluator {

    private final ParquetSourceFile sourceFile;
    private final List<ParquetColumnBinding> columnBindings;


    ParquetPartitionAwareFilterEvaluator( MessageType schema, ParquetValueExtractor valueExtractor, ParquetSourceFile sourceFile, List<ParquetColumnBinding> columnBindings ) {
        super( schema, valueExtractor );
        this.sourceFile = sourceFile;
        this.columnBindings = List.copyOf( columnBindings );
    }


    @Override
    protected Boolean evaluateLeaf( Group group, ParquetAdapterFilter filter ) {
        if ( filter.columnIndex() >= 0 && filter.columnIndex() < columnBindings.size() && columnBindings.get( filter.columnIndex() ).role() == ParquetColumnRole.PARTITION ) {
            return matchesValue( partitionValue( columnBindings.get( filter.columnIndex() ).columnName() ), filter.operator(), filter.polyValue() );
        }
        return super.evaluateLeaf( group, filter );
    }


    PolyValue partitionValue( String columnName ) {
        if ( !sourceFile.partitionValues().containsKey( columnName ) ) {
            return org.polypheny.db.type.entity.PolyNull.NULL;
        }
        return PolyString.of( sourceFile.partitionValues().get( columnName ) );
    }

}
