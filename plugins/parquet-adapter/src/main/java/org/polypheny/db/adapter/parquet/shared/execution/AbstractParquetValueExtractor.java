package org.polypheny.db.adapter.parquet.shared.execution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

/**
 * Conversion base class for mapping
 * Parquet primitive and structured values into Polypheny values
 */
public abstract class AbstractParquetValueExtractor implements ParquetValueExtractor {

    protected final ParquetTypeConverter typeConverter = new ParquetTypeConverter();


    protected PolyValue extractStructuredValue( Group group, int index, Type field ) {
        int repetitionCount = group.getFieldRepetitionCount( index );
        if ( repetitionCount == 0 ) {
            return PolyNull.NULL;
        }

        if ( repetitionCount > 1 ) {
            List<PolyValue> values = new ArrayList<>( repetitionCount );
            for ( int occurrence = 0; occurrence < repetitionCount; occurrence++ ) {
                values.add( extractStructuredValue( group, index, field, occurrence ) );
            }
            return PolyList.of( values );
        }

        return extractStructuredValue( group, index, field, 0 );
    }


    protected PolyValue extractStructuredValue( Group group, int index, Type field, int occurrence ) {
        if ( field.isPrimitive() ) {
            return typeConverter.fromObjToPolyValue( field, extractPrimitiveValue( group, index, field.asPrimitiveType(), occurrence ) );
        }

        return extractNestedDocument( group.getGroup( index, occurrence ), field.asGroupType() );
    }


    protected PolyDocument extractNestedDocument( Group group, GroupType type ) {
        Map<PolyString, PolyValue> values = new LinkedHashMap<>();
        for ( int i = 0; i < type.getFieldCount(); i++ ) {
            Type nestedField = type.getType( i );
            if ( group.getFieldRepetitionCount( i ) == 0 ) {
                continue;
            }
            values.put( PolyString.of( AbstractParquetSource.normalizeFieldName( nestedField.getName() ) ), extractStructuredValue( group, i, nestedField ) );
        }
        return PolyDocument.ofDocument( values );
    }


    protected Object extractPrimitiveValue( Group group, int index, PrimitiveType primitive, int occurrence ) {
        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> group.getBoolean( index, occurrence );
            case INT32 -> group.getInteger( index, occurrence );
            case INT64 -> group.getLong( index, occurrence );
            case FLOAT -> group.getFloat( index, occurrence );
            case DOUBLE -> group.getDouble( index, occurrence );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> group.getBinary( index, occurrence );
        };
    }

}
