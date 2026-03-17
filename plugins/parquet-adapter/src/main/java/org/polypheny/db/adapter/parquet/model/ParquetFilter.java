package org.polypheny.db.adapter.parquet.model;

import org.polypheny.db.algebra.constant.Kind;

/**
 * Immutable Filter description
 * @param columnIndex - index of filter column
 * @param operator - filter operation
 * @param literalValue - filter value
 */
public record ParquetFilter(int columnIndex, Kind operator, String literalValue) {

}
