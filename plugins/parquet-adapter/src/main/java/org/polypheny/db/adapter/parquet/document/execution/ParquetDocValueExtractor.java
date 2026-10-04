package org.polypheny.db.adapter.parquet.document.execution;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetValueExtractor;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

/**
 * Converts Parquet groups and nested values into Polypheny document values and
 * synthesizes `_id` values when they are missing.
 */
public class ParquetDocValueExtractor extends AbstractParquetValueExtractor {

    private static final PolyString ID_KEY = PolyString.of( "_id" );


    @Override
    public PolyValue extractValue( Group group, int index, Type type ) {
        return extractStructuredValue( group, index, type );
    }


    @Override
    public PolyValue extractValue( Group group, List<String> path ) {
        throw new UnsupportedOperationException( ParquetDocValueExtractor.class.getName() + " does not support extracting value via path." );
    }


    /**
     * Convert one Parquet row (Group) into one Polypheny document (PolyDocument)
     *
     * @param group - parquet row
     * @param schema - file schema (fields)
     * @param generatedId - id for the row
     * @return - document
     */
    public PolyDocument extractDocument( Group group, MessageType schema, PolyString generatedId ) {
        Map<PolyString, PolyValue> values = new LinkedHashMap<>();

        for ( int i = 0; i < schema.getFieldCount(); i++ ) {
            Type field = schema.getType( i );
            // If repetition count = 0, the field is absent in this row, so skip it
            if ( group.getFieldRepetitionCount( i ) == 0 ) {
                continue;
            }
            // store result in the map under the normalized name
            values.put( PolyString.of( ParquetNameNormalizer.normalizeFieldName( field.getName() ) ), extractValue( group, i, field ) );
        }

        // if _id is missing or null, insert the provided generatedId
        if ( !values.containsKey( ID_KEY ) || values.get( ID_KEY ).isNull() ) {
            values.put( ID_KEY, generatedId );
        }

        return PolyDocument.ofDocument( values );
    }

}
