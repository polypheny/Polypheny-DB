package org.polypheny.db.adapter.parquet.document.execution;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetValueExtractor;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;


public class ParquetDocValueExtractor extends AbstractParquetValueExtractor {

    @Override
    public PolyValue extractValue( Group group, int index, Type type ) {
        return extractStructuredValue( group, index, type );
    }


    public PolyDocument extractDocument( Group group, MessageType schema, PolyString generatedId ) {
        Map<PolyString, PolyValue> values = new LinkedHashMap<>();

        for ( int i = 0; i < schema.getFieldCount(); i++ ) {
            Type field = schema.getType( i );
            if ( group.getFieldRepetitionCount( i ) == 0 ) {
                continue;
            }
            values.put( PolyString.of( AbstractParquetSource.normalizeFieldName( field.getName() ) ), extractValue( group, i, field ) );
        }

        PolyString idKey = PolyString.of( "_id" );
        if ( !values.containsKey( idKey ) || values.get( idKey ).isNull() ) {
            values.put( idKey, generatedId );
        }

        return PolyDocument.ofDocument( values );
    }

}
