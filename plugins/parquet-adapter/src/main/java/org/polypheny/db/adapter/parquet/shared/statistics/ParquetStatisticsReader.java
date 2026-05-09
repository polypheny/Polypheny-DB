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
import java.util.Set;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.statistics.ProvidedColumnStatistics;
import org.polypheny.db.adapter.statistics.ProvidedEntityStatistics;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Provides parquet statistics
 */
public class ParquetStatisticsReader {

    private final ParquetSchemaReader schemaReader;
    private final ParquetTableBinding binding;
    private final ParquetTypeConverter typeConverter;


    public ParquetStatisticsReader( ParquetSchemaReader schemaReader, ParquetTableBinding binding ) {
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
        return Optional.of( new ProvidedEntityStatistics( nestedTable ? estimateNestedRowCount() : schemaReader.getEstimatedRowCount() ) );
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

        PrimitiveType primitiveType;
        try {
            primitiveType = schemaReader.getSchema().getType( columnBinding.sourcePathElements().toArray( new String[0] ) ).asPrimitiveType();
        } catch ( RuntimeException e ) {
            return Optional.of( new ProvidedColumnStatistics( estimateEntityRowCount(), null, null, List.of(), true ) );
        }

        ColumnMetadataStatistics metadataStatistics = readColumnMetadataStatistics( columnBinding.sourcePathElements() );
        PolyValue min = toStatisticValue( column.type, primitiveType, metadataStatistics.min() );
        PolyValue max = toStatisticValue( column.type, primitiveType, metadataStatistics.max() );
        return Optional.of( new ProvidedColumnStatistics( metadataStatistics.count(), min, max, List.of(), true ) );
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
        return binding.parentTableName() == null ? schemaReader.getEstimatedRowCount() : estimateNestedRowCount();
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
        for ( var footer : schemaReader.getFooters() ) {
            for ( BlockMetaData block : footer.getBlocks() ) {
                ColumnChunkMetaData column = findColumnChunk( block, sourcePathElements );
                if ( column == null ) {
                    continue;
                }
                valueCount += column.getValueCount();
            }
        }
        return valueCount;
    }


    /**
     * Count rows including min/max limits
     * @param sourcePathElements - path
     * @return column metadata statistics
     */
    private ColumnMetadataStatistics readColumnMetadataStatistics( List<String> sourcePathElements ) {
        long rowCount = 0;
        long nullCount = 0;
        boolean hasReliableNullCount = true;
        Object min = null;
        Object max = null;
        boolean hasMinMax = true;

        for ( var footer : schemaReader.getFooters() ) {
            for ( BlockMetaData block : footer.getBlocks() ) {
                rowCount += block.getRowCount();
                ColumnChunkMetaData column = findColumnChunk( block, sourcePathElements );
                if ( column == null ) {
                    nullCount += block.getRowCount();
                    continue;
                }

                Statistics<?> statistics = column.getStatistics();
                if ( statistics == null ) {
                    hasReliableNullCount = false;
                    hasMinMax = false;
                    continue;
                }

                if ( statistics.isNumNullsSet() ) {
                    nullCount += statistics.getNumNulls();
                } else {
                    hasReliableNullCount = false;
                }

                if ( statistics.hasNonNullValue() ) {
                    Object currentMin = statistics.genericGetMin();
                    Object currentMax = statistics.genericGetMax();
                    min = lower( min, currentMin );
                    max = higher( max, currentMax );
                } else if ( !statistics.isNumNullsSet() || statistics.getNumNulls() != block.getRowCount() ) {
                    hasMinMax = false;
                }
            }
        }

        long count = hasReliableNullCount ? rowCount - nullCount : rowCount;
        return new ColumnMetadataStatistics( count, hasMinMax ? min : null, hasMinMax ? max : null );
    }


    private ColumnChunkMetaData findColumnChunk( BlockMetaData block, List<String> sourcePathElements ) {
        for ( ColumnChunkMetaData column : block.getColumns() ) {
            if ( List.of( column.getPath().toArray() ).equals( sourcePathElements ) ) {
                return column;
            }
        }
        return null;
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Object lower( Object current, Object candidate ) {
        if ( current == null ) {
            return candidate;
        }
        return ((Comparable) candidate).compareTo( current ) < 0 ? candidate : current;
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Object higher( Object current, Object candidate ) {
        if ( current == null ) {
            return candidate;
        }
        return ((Comparable) candidate).compareTo( current ) > 0 ? candidate : current;
    }


    private PolyValue toStatisticValue( PolyType columnType, PrimitiveType primitiveType, Object value ) {
        if ( value == null ) {
            return null;
        }
        try {
            PolyValue polyValue = typeConverter.fromObjToPolyValue( primitiveType, value );
            if ( columnType.getFamily() == PolyTypeFamily.NUMERIC && polyValue.isNumber() ) {
                return polyValue;
            }
            if ( PolyType.DATETIME_TYPES.contains( columnType ) && polyValue.isTemporal() ) {
                return polyValue;
            }
            if ( columnType.getFamily() == PolyTypeFamily.CHARACTER && polyValue.isString() ) {
                return polyValue;
            }
            return null;
        } catch ( RuntimeException e ) {
            return null;
        }
    }


    private record ColumnMetadataStatistics(
            Long count,
            Object min,
            Object max ) {

    }

}
