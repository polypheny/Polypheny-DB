package org.polypheny.db.adapter.parquet.execution;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Central conversion layer between Parquet and Polypheny value representations.
 */
public class ValueExtractor {

    public Object extractValue( Group group, int index, Type field ) {
        if ( !field.isPrimitive() ) {
            return group.getValueToString( index, 0 );
        }

        PrimitiveType primitive = field.asPrimitiveType();

        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> group.getBoolean( index, 0 );
            case INT32 -> group.getInteger( index, 0 );
            case INT64 -> group.getLong( index, 0 );
            case FLOAT -> group.getFloat( index, 0 );
            case DOUBLE -> group.getDouble( index, 0 );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> group.getBinary( index, 0 );
        };
    }
}
