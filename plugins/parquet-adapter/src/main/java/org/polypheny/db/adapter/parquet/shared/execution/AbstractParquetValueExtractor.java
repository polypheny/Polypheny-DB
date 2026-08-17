package org.polypheny.db.adapter.parquet.shared.execution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
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

        if ( isList( field ) ) {
            return extractListValue( group, index, field.asGroupType() );
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


    private boolean isList( Type field ) {
        return !field.isPrimitive() && field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    }


    private PolyList<PolyValue> extractListValue( Group group, int index, GroupType listType ) {
        List<PolyValue> values = new ArrayList<>();
        int listOccurrences = group.getFieldRepetitionCount( index );
        for ( int listOccurrence = 0; listOccurrence < listOccurrences; listOccurrence++ ) {
            Group listGroup = group.getGroup( index, listOccurrence );
            if ( listType.getFieldCount() == 0 ) {
                continue;
            }
            Type repeated = listType.getType( 0 );
            int elementOccurrences = listGroup.getFieldRepetitionCount( 0 );
            for ( int elementOccurrence = 0; elementOccurrence < elementOccurrences; elementOccurrence++ ) {
                values.add( extractListElement( listGroup, repeated, elementOccurrence ) );
            }
        }
        return PolyList.of( values );
    }


    private PolyValue extractListElement( Group listGroup, Type repeated, int occurrence ) {
        if ( repeated.isPrimitive() ) {
            return extractStructuredValue( listGroup, 0, repeated, occurrence );
        }

        Group repeatedGroup = listGroup.getGroup( 0, occurrence );
        GroupType repeatedType = repeated.asGroupType();
        if ( repeatedType.getFieldCount() == 1 && "element".equals( repeatedType.getType( 0 ).getName() ) ) {
            return extractStructuredValue( repeatedGroup, 0, repeatedType.getType( 0 ) );
        }
        return extractNestedDocument( repeatedGroup, repeatedType );
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
            values.put( PolyString.of( ParquetNameNormalizer.normalizeFieldName( nestedField.getName() ) ), extractStructuredValue( group, i, nestedField ) );
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
