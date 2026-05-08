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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetEnumerator;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Source;

/**
 * Used to handle virtual table that was created from nested non-repeated types.
 */
public class ParquetNestedNonRepeatedRelEnumerator extends AbstractParquetEnumerator {

    private final List<ParquetColumnBinding> columnBindings;


    public ParquetNestedNonRepeatedRelEnumerator( Source source, AtomicBoolean cancelFlag, List<ParquetColumnBinding> columnBindings ) {
        this( source, cancelFlag, columnBindings, FiltersContainer.empty );
    }


    public ParquetNestedNonRepeatedRelEnumerator( Source source, AtomicBoolean cancelFlag, List<ParquetColumnBinding> columnBindings, FiltersContainer filtersContainer ) {
        // read full root rows from the Parquet file
        super( source, cancelFlag, null, filtersContainer, new ParquetPathValueExtractor() );
        this.columnBindings = List.copyOf( columnBindings );
    }


    @Override
    protected PolyValue[] extractRow( Group group ) {
        PolyValue[] row = new PolyValue[columnBindings.size()];
        for ( int i = 0; i < columnBindings.size(); i++ ) {
            row[i] = extractValueByColumnRole( (VirtualGroup) group, columnBindings.get( i ) );
        }
        return row;
    }


    @Override
    protected List<Group> expandRow( Group group ) {
        var virtualGroup = new VirtualGroup( group, String.valueOf( reader.getCurrentRowNumber() ), null, 0 );
        return super.expandRow( virtualGroup );
    }


    private PolyValue extractValueByColumnRole( VirtualGroup virtualGroup, ParquetColumnBinding binding ) {
        return switch ( binding.role() ) {
            case DATA -> valueExtractor.extractValue( virtualGroup, binding.sourcePathElements() );
            case PRIMARY_KEY -> PolyString.of( virtualGroup.getMetadata().getRowId() );
            case PARENT_KEY -> virtualGroup.getMetadata().getParentRowId() == null
                    ? PolyNull.NULL
                    : PolyString.of( virtualGroup.getMetadata().getParentRowId() );
            case ORDINAL -> PolyLong.of( virtualGroup.getMetadata().getOrdinal() );
        };
    }

}
