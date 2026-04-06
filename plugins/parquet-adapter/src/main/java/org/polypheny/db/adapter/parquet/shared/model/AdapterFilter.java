package org.polypheny.db.adapter.parquet.shared.model;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Immutable Filter description
 * @param columnIndex - index of filter column
 * @param operator - filter operation
 * @param polyValue - filter value
 */
public record AdapterFilter(int columnIndex, Kind operator, PolyValue polyValue, Long dynamicParamIndex ) {

    public AdapterFilter( int columnIndex, Kind operator, PolyValue polyValue ) {
        this( columnIndex, operator, polyValue, null );
    }

}
