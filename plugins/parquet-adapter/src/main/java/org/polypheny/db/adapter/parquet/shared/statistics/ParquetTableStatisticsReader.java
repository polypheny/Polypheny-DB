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

package org.polypheny.db.adapter.parquet.shared.statistics;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.statistics.ProvidedColumnStatistics;
import org.polypheny.db.adapter.statistics.ProvidedEntityStatistics;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Provides parquet statistics
 */
public class ParquetTableStatisticsReader {

    private final ParquetSchemaReader schemaReader;
    private final ParquetTableBinding binding;
    private final ParquetTypeConverter typeConverter;


    public ParquetTableStatisticsReader( ParquetSchemaReader schemaReader, ParquetTableBinding binding ) {
        this.schemaReader = schemaReader;
        this.binding = binding;
        this.typeConverter = new ParquetTypeConverter();
    }


    /**
     * Get number of rows in table.
     * @param nestedTable - if nested table
     * @return provided statistics
     */
    public Optional<ProvidedEntityStatistics> getEntityStatistics( boolean nestedTable ) {
        return Optional.of( new ProvidedEntityStatistics( nestedTable ? estimateNestedRowCount() : estimateSourceRowCount() ) );
    }


    /**
     * Provides the available metadata-based statistics for one logical column, without scanning the table rows.
     * @param column - logical column
     * @param uniqueValueLimit - currently not used, required by interface
     * @return Column Statistics
     */
    public Optional<ProvidedColumnStatistics> getColumnStatistics( LogicalColumn column, int uniqueValueLimit ) {
        ParquetColumnBinding columnBinding = binding.getColumnBinding( column.id );
        if ( columnBinding == null ) {
            return Optional.of( new ProvidedColumnStatistics( estimateEntityRowCount(), null, null, List.of(), true ) );
        }
        if ( columnBinding.role() == ParquetColumnRole.PARTITION ) {
            return Optional.of( partitionColumnStatistics( columnBinding, uniqueValueLimit ) );
        }
        if ( columnBinding.role() != ParquetColumnRole.DATA || columnBinding.sourcePathElements().isEmpty() ) {
            return Optional.of( new ProvidedColumnStatistics( estimateEntityRowCount(), null, null, List.of(), true ) );
        }

        Optional<ParquetColumnStatistics> metadataStatistics = aggregateColumnStatistics( columnBinding.sourcePathElements() );
        if ( metadataStatistics.isEmpty() ) {
            return Optional.of( new ProvidedColumnStatistics( estimateEntityRowCount(), null, null, List.of(), true ) );
        }

        ParquetColumnStatistics statistics = metadataStatistics.get();
        PolyValue min = statistics.hasRange() ? typeConverter.fromStringToCompatiblePolyValue( column.type, statistics.type(), statistics.min() ) : null;
        PolyValue max = statistics.hasRange() ? typeConverter.fromStringToCompatiblePolyValue( column.type, statistics.type(), statistics.max() ) : null;
        return Optional.of( new ProvidedColumnStatistics( nonNullCount( statistics ), min, max, List.of(), true ) );
    }


    ProvidedColumnStatistics partitionColumnStatistics( ParquetColumnBinding columnBinding, int uniqueValueLimit ) {
        Set<String> values = new LinkedHashSet<>();
        for ( ParquetSourceFile sourceFile : binding.sourceFiles() ) {
            String value = sourceFile.partitionValues().get( columnBinding.columnName() );
            if ( value != null ) {
                values.add( value );
            }
        }

        if ( uniqueValueLimit > 0 && values.size() > uniqueValueLimit ) {
            return new ProvidedColumnStatistics( estimateEntityRowCount(), null, null, List.of(), true );
        }

        List<PolyValue> uniqueValues = values.stream()
                .map( PolyString::of )
                .map( PolyValue.class::cast )
                .toList();
        PolyValue min = values.stream().min( Comparator.naturalOrder() ).map( PolyString::of ).orElse( null );
        PolyValue max = values.stream().max( Comparator.naturalOrder() ).map( PolyString::of ).orElse( null );
        return new ProvidedColumnStatistics( estimateEntityRowCount(), min, max, uniqueValues, false );
    }


    private long estimateEntityRowCount() {
        return binding.parentTableName() == null ? estimateSourceRowCount() : estimateNestedRowCount();
    }


    private long estimateSourceRowCount() {
        long rowCount = 0;
        for ( ParquetSourceFile sourceFile : binding.sourceFiles() ) {
            OptionalLong fileRowCount = sourceRowCount( sourceFile );
            if ( fileRowCount.isEmpty() ) {
                return schemaReader.getEstimatedRowCount();
            }
            rowCount += fileRowCount.getAsLong();
        }
        return rowCount;
    }


    /**
     * Estimates how many rows a normalized nested table will produce, using Parquet metadata only.
     * @return estimated Nested Row Count
     */
    private long estimateNestedRowCount() {
        return binding.columnsByColumnId().values().stream()
                .filter( column -> column.role() == ParquetColumnRole.DATA )
                .filter( column -> !column.sourcePathElements().isEmpty() )
                .mapToLong( column -> estimateValueCount( column.sourcePathElements() ) )
                .max()
                .orElseGet( schemaReader::getEstimatedRowCount );
    }


    private long estimateValueCount( List<String> sourcePathElements ) {
        long valueCount = 0;
        for ( ParquetSourceFile sourceFile : binding.sourceFiles() ) {
            ParquetColumnStatistics statistics = sourceFile.columnStatistics().get( sourcePathElements );
            if ( statistics != null ) {
                valueCount += statistics.valueCount();
            }
        }
        return valueCount;
    }


    private Optional<ParquetColumnStatistics> aggregateColumnStatistics( List<String> sourcePathElements ) {
        PolyType type = null;
        long rowCount = 0;
        long valueCount = 0;
        Long nullCount = 0L;
        String min = null;
        String max = null;
        boolean minMaxReliable = true;
        boolean hasColumnStatistics = false;

        for ( ParquetSourceFile sourceFile : binding.sourceFiles() ) {
            ParquetColumnStatistics statistics = sourceFile.columnStatistics().get( sourcePathElements );
            if ( statistics == null ) {
                OptionalLong fileRowCount = sourceRowCount( sourceFile );
                if ( fileRowCount.isEmpty() ) {
                    nullCount = null;
                    minMaxReliable = false;
                    continue;
                }
                long missingRows = fileRowCount.getAsLong();
                rowCount += missingRows;
                valueCount += missingRows;
                if ( nullCount != null ) {
                    nullCount += missingRows;
                }
                continue;
            }

            hasColumnStatistics = true;
            if ( type == null ) {
                type = statistics.type();
            } else if ( type != statistics.type() ) {
                minMaxReliable = false;
            }

            rowCount += statistics.rowCount();
            valueCount += statistics.valueCount();
            if ( nullCount != null ) {
                nullCount = statistics.nullCount() == null ? null : nullCount + statistics.nullCount();
            }

            if ( !statistics.minMaxReliable() ) {
                minMaxReliable = false;
            } else if ( statistics.hasRange() && type == statistics.type() ) {
                min = lowerStatisticValue( min, statistics.min(), type );
                max = higherStatisticValue( max, statistics.max(), type );
            } else if ( !statistics.hasOnlyNulls() ) {
                minMaxReliable = false;
            }
        }

        if ( !hasColumnStatistics || type == null ) {
            return Optional.empty();
        }
        return Optional.of( new ParquetColumnStatistics( type, rowCount, valueCount, nullCount, min, max, minMaxReliable ) );
    }


    private OptionalLong sourceRowCount( ParquetSourceFile sourceFile ) {
        return sourceFile.columnStatistics().values().stream()
                .mapToLong( ParquetColumnStatistics::rowCount )
                .findFirst();
    }


    private long nonNullCount( ParquetColumnStatistics statistics ) {
        return statistics.nullCount() == null ? statistics.rowCount() : statistics.rowCount() - statistics.nullCount();
    }


    private String lowerStatisticValue( String current, String candidate, PolyType type ) {
        if ( current == null ) {
            return candidate;
        }
        return typeConverter.compareStringValues( type, candidate, current ) < 0 ? candidate : current;
    }


    private String higherStatisticValue( String current, String candidate, PolyType type ) {
        if ( current == null ) {
            return candidate;
        }
        return typeConverter.compareStringValues( type, candidate, current ) > 0 ? candidate : current;
    }

}
