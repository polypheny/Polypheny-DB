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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;


/**
 * Reads compact column statistics from Parquet footer metadata.
 * Returns ParquetColumnStatistics per column
 */
public final class ParquetColumnStatisticsReader {

    private static final ParquetTypeConverter TYPE_CONVERTER = new ParquetTypeConverter();


    private ParquetColumnStatisticsReader() {
    }


    public static Map<List<String>, ParquetColumnStatistics> readAll( Source source ) {
        return readAll( new ParquetSchemaReader( source ) );
    }


    public static Map<List<String>, ParquetColumnStatistics> readAll( ParquetSchemaReader schemaReader ) {
        Map<List<String>, StatisticsBuilder> statistics = new LinkedHashMap<>();

        for ( var footer : schemaReader.getFooters() ) {
            for ( BlockMetaData block : footer.getBlocks() ) {
                for ( ColumnChunkMetaData column : block.getColumns() ) {
                    List<String> path = List.of( column.getPath().toArray() );
                    PrimitiveType type = primitiveType( schemaReader, path );
                    if ( type == null ) {
                        continue;
                    }
                    statistics.computeIfAbsent( path, ignored -> new StatisticsBuilder( type ) )
                            .add( block.getRowCount(), column );
                }
            }
        }

        Map<List<String>, ParquetColumnStatistics> result = new LinkedHashMap<>();
        statistics.forEach( ( path, builder ) -> result.put( path, builder.build() ) );
        return result;
    }


    public static Optional<ParquetColumnStatistics> read( ParquetSchemaReader schemaReader, List<String> path ) {
        return read( schemaReader, path, true );
    }


    private static Optional<ParquetColumnStatistics> read( ParquetSchemaReader schemaReader, List<String> path, boolean missingColumnsAreNull ) {
        PrimitiveType type = primitiveType( schemaReader, path );
        if ( type == null ) {
            return Optional.empty();
        }

        StatisticsBuilder builder = new StatisticsBuilder( type );
        for ( var footer : schemaReader.getFooters() ) {
            for ( BlockMetaData block : footer.getBlocks() ) {
                ColumnChunkMetaData column = findColumnChunk( block, path );
                if ( column == null ) {
                    if ( missingColumnsAreNull ) {
                        builder.addMissing( block.getRowCount() );
                    }
                    continue;
                }
                builder.add( block.getRowCount(), column );
            }
        }
        return Optional.of( builder.build() );
    }


    private static PrimitiveType primitiveType( ParquetSchemaReader schemaReader, List<String> path ) {
        try {
            return schemaReader.getSchema().getType( path.toArray( String[]::new ) ).asPrimitiveType();
        } catch ( RuntimeException e ) {
            return null;
        }
    }


    private static ColumnChunkMetaData findColumnChunk( BlockMetaData block, List<String> path ) {
        for ( ColumnChunkMetaData column : block.getColumns() ) {
            if ( List.of( column.getPath().toArray() ).equals( path ) ) {
                return column;
            }
        }
        return null;
    }


    private static final class StatisticsBuilder {

        private final PrimitiveType type;
        private final PolyType polyType;
        private long rowCount;
        private long valueCount;
        private Long nullCount = 0L;
        private Object min;
        private Object max;
        private boolean minMaxReliable = true;


        private StatisticsBuilder( PrimitiveType type ) {
            this.type = type;
            this.polyType = TYPE_CONVERTER.fromParquetTypeToPolyType( type );
        }


        private void add( long blockRowCount, ColumnChunkMetaData column ) {
            rowCount += blockRowCount;
            valueCount += column.getValueCount();

            Statistics<?> statistics = column.getStatistics();
            if ( statistics == null ) {
                nullCount = null;
                minMaxReliable = false;
                return;
            }

            if ( nullCount != null ) {
                if ( statistics.isNumNullsSet() ) {
                    nullCount += statistics.getNumNulls();
                } else {
                    nullCount = null;
                }
            }

            if ( statistics.hasNonNullValue() ) {
                min = lower( min, statistics.genericGetMin() );
                max = higher( max, statistics.genericGetMax() );
                return;
            }

            if ( !statistics.isNumNullsSet() || statistics.getNumNulls() != column.getValueCount() ) {
                minMaxReliable = false;
            }
        }


        private void addMissing( long blockRowCount ) {
            rowCount += blockRowCount;
            valueCount += blockRowCount;
            if ( nullCount != null ) {
                nullCount += blockRowCount;
            }
        }


        private ParquetColumnStatistics build() {
            String serializedMin = statisticValue( min );
            String serializedMax = statisticValue( max );
            boolean reliableRange = minMaxReliable && (min == null || serializedMin != null) && (max == null || serializedMax != null);
            return new ParquetColumnStatistics( polyType, rowCount, valueCount, nullCount, serializedMin, serializedMax, reliableRange );
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


        private String statisticValue( Object value ) {
            return TYPE_CONVERTER.fromParquetValueToString( type, value );
        }

    }

}
