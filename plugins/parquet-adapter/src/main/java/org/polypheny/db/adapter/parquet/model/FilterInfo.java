package org.polypheny.db.adapter.parquet.model;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Immutable Filter description
 * @param columnIndex - index of filter column
 * @param operator - filter operation
 * @param polyValue - filter value
 */
public record FilterInfo(int columnIndex, Kind operator, PolyValue polyValue ) {

}
