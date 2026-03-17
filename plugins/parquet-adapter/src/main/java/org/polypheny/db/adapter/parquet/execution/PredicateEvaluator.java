package org.polypheny.db.adapter.parquet.execution;

import java.util.List;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.model.ParquetFilter;
import org.polypheny.db.adapter.parquet.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;

/**
 * Checks whether a row group can be skipped for a given filter (predicate)
 */
public class PredicateEvaluator {

    private final ParquetTypeConverter typeConverter = new ParquetTypeConverter();


    /**
     * Returns whether the row group may contain matches.
     */
    public boolean mightContain( BlockMetaData block, MessageType schema, List<ParquetFilter> filters ) {
        if ( filters == null || filters.isEmpty() ) {
            return true;
        }

        for ( ParquetFilter filter : filters ) {
            int i = filter.columnIndex();
            if ( i < 0 || i >= schema.getFieldCount() ) {
                continue;
            }

            Type type = schema.getType( i );
            if ( !type.isPrimitive() ) {
                continue;
            }

            PrimitiveType primitive = type.asPrimitiveType();
            ColumnChunkMetaData column = findColumn( block.getColumns(), schema.getFieldName( i ) );
            if ( column == null ) {
                continue;
            }

            Statistics<?> stats = column.getStatistics();
            if ( stats == null || stats.isEmpty() ) {
                continue;
            }

            long rowCount = block.getRowCount();
            long nullCount = stats.isNumNullsSet() ? stats.getNumNulls() : -1L;
            if ( nullCount == rowCount && rowCount > 0 ) {
                return false;
            }

            Object expected = typeConverter.fromLiteralToPrimitive( primitive, filter.literalValue() );
            if ( expected == null ) {
                continue;
            }

            if ( !stats.hasNonNullValue() ) {
                return false;
            }

            Object min = stats.genericGetMin();
            Object max = stats.genericGetMax();
            if ( min == null || max == null ) {
                continue;
            }

            Integer cmpMin = ValueComparator.compareValues( primitive, expected, min );
            Integer cmpMax = ValueComparator.compareValues( primitive, expected, max );
            if ( cmpMin == null || cmpMax == null ) {
                continue;
            }

            if ( canSkipBlock( filter.operator(), primitive, cmpMin, cmpMax, min, max, expected, rowCount, nullCount ) ) {
                return false;
            }
        }

        return true;
    }


    /**
     * Applies operator-specific pruning rules.
     */
    private boolean canSkipBlock( Kind operator, PrimitiveType primitive, int cmpMin, int cmpMax, Object min, Object max, Object expected, long rowCount, long nullCount ) {
        return switch ( operator ) {
            case EQUALS -> cmpMin < 0 || cmpMax > 0;
            case GREATER_THAN -> cmpMax >= 0;
            case GREATER_THAN_OR_EQUAL -> cmpMax > 0;
            case LESS_THAN -> cmpMin <= 0;
            case LESS_THAN_OR_EQUAL -> cmpMin < 0;
            case NOT_EQUALS -> {
                Integer cmpMinMax = ValueComparator.compareValues( primitive, min, max );
                Integer cmpMinExpected = ValueComparator.compareValues( primitive, min, expected );
                yield cmpMinMax != null && cmpMinExpected != null && cmpMinMax == 0 && cmpMinExpected == 0 && nullCount == 0 && rowCount > 0;
            }
            default -> false;
        };
    }


    /**
     * Finds column metadata by top-level Parquet field name.
     */
    private ColumnChunkMetaData findColumn( List<ColumnChunkMetaData> columns, String fieldName ) {
        for ( ColumnChunkMetaData column : columns ) {
            if ( column.getPath() != null && fieldName.equals( column.getPath().toDotString() ) ) {
                return column;
            }
        }
        return null;
    }
}
