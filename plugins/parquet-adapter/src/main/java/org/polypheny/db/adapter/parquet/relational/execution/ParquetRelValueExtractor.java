package org.polypheny.db.adapter.parquet.relational.execution;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetValueExtractor;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Converts Parquet field values into Polypheny relational values
 * with relational-specific scalar handling.
 */
public class ParquetRelValueExtractor extends AbstractParquetValueExtractor {

    @Override
    public PolyValue extractValue( Group group, int index, Type type ) {
        if ( group.getFieldRepetitionCount( index ) == 0 ) {
            return PolyNull.NULL;
        }
        if ( !type.isPrimitive() || group.getFieldRepetitionCount( index ) > 1 ) {
            return extractStructuredValue( group, index, type );
        }
        PrimitiveType primitive = type.asPrimitiveType();
        Object value = extractPrimitiveValue( group, index, primitive, 0 );
        return typeConverter.fromObjToPolyValue( type, value );
    }

}
