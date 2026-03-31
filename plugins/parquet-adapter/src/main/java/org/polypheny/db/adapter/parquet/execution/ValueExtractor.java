package org.polypheny.db.adapter.parquet.execution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.schema.ParquetTypeConverter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;


/**
 * Central conversion layer between Parquet and Polypheny value representations.
 */
public class ValueExtractor {

    private final ParquetTypeConverter typeConverter = new ParquetTypeConverter();

    /*
    The function reads one Parquet field from the current row.
    If the field is primitive, function returns the primitive Java value.
    If the field is nested, it returns JSON text built from the nested group.
     */
    public PolyValue extractValue( Group group, int index, Type type ) {
        if ( !type.isPrimitive() ) {
            return PolyString.of( toJson( group.getGroup( index, 0 ) ));
        }
        PrimitiveType primitive = type.asPrimitiveType();
        Object value = extractPrimitiveValue( group, index, primitive, 0 );
        return typeConverter.fromObjToPolyValue( type, value );
    }


    public PolyDocument extractDocument( Group group, MessageType schema, PolyString generatedId ) {
        Map<PolyString, PolyValue> values = new LinkedHashMap<>();

        for ( int i = 0; i < schema.getFieldCount(); i++ ) {
            Type field = schema.getType( i );
            if ( group.getFieldRepetitionCount( i ) == 0 ) {
                continue;
            }
            values.put( PolyString.of( field.getName() ), extractDocumentValue( group, i, field ) );
        }

        PolyString idKey = PolyString.of( "_id" );
        if ( !values.containsKey( idKey ) || values.get( idKey ).isNull() ) {
            values.put( idKey, generatedId );
        }

        return PolyDocument.ofDocument( values );
    }


    public PolyValue extractDocumentValue( Group group, int index, Type field ) {
        int repetitionCount = group.getFieldRepetitionCount( index );
        if ( repetitionCount == 0 ) {
            return PolyNull.NULL;
        }

        if ( repetitionCount > 1 ) {
            List<PolyValue> values = new ArrayList<>( repetitionCount );
            for ( int occurrence = 0; occurrence < repetitionCount; occurrence++ ) {
                values.add( extractDocumentValue( group, index, field, occurrence ) );
            }
            return PolyList.of( values );
        }

        return extractDocumentValue( group, index, field, 0 );
    }


    private PolyValue extractDocumentValue( Group group, int index, Type field, int occurrence ) {
        if ( field.isPrimitive() ) {
            return typeConverter.fromObjToPolyValue( field, extractPrimitiveValue( group, index, field.asPrimitiveType(), occurrence ) );
        }

        return extractNestedDocument( group.getGroup( index, occurrence ), field.asGroupType() );
    }


    private PolyDocument extractNestedDocument( Group group, GroupType type ) {
        Map<PolyString, PolyValue> values = new LinkedHashMap<>();
        for ( int i = 0; i < type.getFieldCount(); i++ ) {
            Type nestedField = type.getType( i );
            if ( group.getFieldRepetitionCount( i ) == 0 ) {
                continue;
            }
            values.put( PolyString.of( nestedField.getName() ), extractDocumentValue( group, i, nestedField ) );
        }
        return PolyDocument.ofDocument( values );
    }


    private Object extractPrimitiveValue( Group group, int index, PrimitiveType primitive, int occurrence ) {
        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> group.getBoolean( index, occurrence );
            case INT32 -> group.getInteger( index, occurrence );
            case INT64 -> group.getLong( index, occurrence );
            case FLOAT -> group.getFloat( index, occurrence );
            case DOUBLE -> group.getDouble( index, occurrence );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> group.getBinary( index, occurrence );
        };
    }

    /*
    The function converts a nested Parquet group into a JSON object string:
    Iterates over all fields inside that group.
    Skips missing fields.
    For primitive nested fields, delegates to toJsonPrimitive(...).
    For nested sub-groups, calls itself recursively.
     */
    private String toJson( Group group ) {
        StringBuilder json = new StringBuilder();
        json.append( "{" );

        String sep = "";
        for ( int i = 0; i < group.getType().getFieldCount(); i++ ) {
            Type field = group.getType().getType( i );
            String name = field.getName();

            if ( group.getFieldRepetitionCount( i ) == 0 ) {
                continue;
            }

            json.append( sep ).append( "\"" ).append( name ).append( "\":" );

            if ( field.isPrimitive() ) {
                json.append( toJsonPrimitive( group, i, field ) );
            } else {
                json.append( toJson( group.getGroup( i, 0 ) ) );
            }

            sep = ",";
        }

        json.append( "}" );
        return json.toString();
    }

    /*
    Converts one primitive Parquet field into a JSON-compatible string fragment.
     */
    private String toJsonPrimitive( Group group, int index, Type field ) {
        PrimitiveType primitive = field.asPrimitiveType();

        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> String.valueOf( group.getBoolean( index, 0 ) );
            case INT32 -> String.valueOf( group.getInteger( index, 0 ) );
            case INT64 -> String.valueOf( group.getLong( index, 0 ) );
            case FLOAT -> String.valueOf( group.getFloat( index, 0 ) );
            case DOUBLE -> String.valueOf( group.getDouble( index, 0 ) );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    "\"" + escapeJson( group.getBinary( index, 0 ).toStringUsingUTF8() ) + "\"";
        };
    }

    /*
    Escapes characters that would break JSON syntax.
     */
    private String escapeJson( String value ) {
        return value
                .replace( "\\", "\\\\" )
                .replace( "\"", "\\\"" )
                .replace( "\n", "\\n" )
                .replace( "\r", "\\r" )
                .replace( "\t", "\\t" );
    }


}
