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
import org.polypheny.db.adapter.parquet.relational.filter.ParquetPartitionAwareFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetEnumerator;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Used to handle virtual table that was created from nested non-repeated types.
 */
public class ParquetNestedNonRepeatedRelEnumerator extends AbstractParquetEnumerator {

    private final List<ParquetColumnBinding> columnBindings;
    private final ParquetSourceFile sourceFile;


    public ParquetNestedNonRepeatedRelEnumerator( ParquetSourceReader reader, ParquetSourceFile sourceFile, List<ParquetColumnBinding> columnBindings, List<ParquetColumnBinding> filterColumnBindings, FiltersContainer filtersContainer ) {
        // read full root rows from the Parquet file
        super( reader, filtersContainer, new ParquetPartitionAwareFilterEvaluator( reader.getProjectionSchema(), new ParquetPathValueExtractor(), sourceFile, filterColumnBindings ) );
        this.columnBindings = List.copyOf( columnBindings );
        this.sourceFile = sourceFile;
    }


    @Override
    protected PolyValue[] extractRow( Group group ) {
        PolyValue[] row = new PolyValue[columnBindings.size()];
        for ( int i = 0; i < columnBindings.size(); i++ ) {
            var binding = columnBindings.get( i );
            row[i] = binding.role() == ParquetColumnRole.PARTITION
                    ? partitionValue( binding )
                    : pathValueExtractor().extractValue( (VirtualGroup) group, binding, List.of() );
        }
        return row;
    }


    @Override
    protected List<Group> expandRow( Group group ) {
        var virtualGroup = new VirtualGroup( group, String.valueOf( reader.getCurrentRowNumber() ), null, 0 );
        return super.expandRow( virtualGroup );
    }


    private PolyValue partitionValue( ParquetColumnBinding binding ) {
        if ( !sourceFile.partitionValues().containsKey( binding.columnName() ) ) {
            return PolyNull.NULL;
        }
        return PolyString.of( sourceFile.partitionValues().get( binding.columnName() ) );
    }


    private ParquetPathValueExtractor pathValueExtractor() {
        return (ParquetPathValueExtractor) filterEvaluator.valueExtractor();
    }

}
