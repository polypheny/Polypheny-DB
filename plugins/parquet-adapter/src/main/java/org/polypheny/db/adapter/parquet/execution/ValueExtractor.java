package org.polypheny.db.adapter.parquet.execution;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;


/**
 * Central conversion layer between Parquet and Polypheny value representations.
 */
public class ValueExtractor {

    /*
    The function reads one Parquet field from the current row.
    If the field is primitive, function returns the primitive Java value.
    If the field is nested, it returns JSON text built from the nested group.
     */
    public Object extractValue( Group group, int index, Type field ) {
        if ( !field.isPrimitive() ) {
            //return group.getValueToString( index, 0 );
            return toJson( group.getGroup( index, 0 ) );
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
