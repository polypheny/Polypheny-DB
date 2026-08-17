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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.util.Optional;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Resolves columns whose value is constant for every row in one source file.
 */
public class ParquetConstantColumnResolver {

    private final ParquetTypeConverter typeConverter = new ParquetTypeConverter();


    /**
     * Resolves a partition value or a flat physical column with singleton file statistics.
     *
     * @param sourceFile a source file.
     * @param binding a column binding.
     * @return the constant value or an empty optional if the column may contain multiple values.
     */
    public Optional<PolyValue> resolve( ParquetSourceFile sourceFile, ParquetColumnBinding binding ) {
        if ( binding == null ) {
            return Optional.empty();
        }
        if ( binding.role() == ParquetColumnRole.PARTITION ) {
            return Optional.of( partitionValue( sourceFile, binding ) );
        }
        if ( binding.role() != ParquetColumnRole.DATA || binding.sourcePathElements().size() != 1 ) {
            return Optional.empty();
        }
        return resolve( sourceFile.columnStatistics().get( binding.sourcePathElements() ) );
    }


    /**
     * Resolves a constant physical value from footer statistics.
     *
     * @param statistics statistics for one physical column.
     * @return the constant value or an empty optional if the column may contain multiple values.
     */
    public Optional<PolyValue> resolve( ParquetColumnStatistics statistics ) {
        if ( statistics == null || statistics.valueCount() != statistics.rowCount() ) {
            return Optional.empty();
        }
        if ( statistics.hasOnlyNulls() ) {
            return Optional.of( PolyNull.NULL );
        }
        if ( !statistics.hasNoNulls()
                || !statistics.hasRange()
                || typeConverter.compareStringValues( statistics.type(), statistics.min(), statistics.max() ) != 0 ) {
            return Optional.empty();
        }
        return Optional.ofNullable( typeConverter.fromStringToPolyValue( statistics.type(), statistics.min() ) );
    }


    private PolyValue partitionValue( ParquetSourceFile sourceFile, ParquetColumnBinding binding ) {
        if ( !sourceFile.partitionValues().containsKey( binding.columnName() ) ) {
            return PolyNull.NULL;
        }
        return PolyString.of( sourceFile.partitionValues().get( binding.columnName() ) );
    }

}
