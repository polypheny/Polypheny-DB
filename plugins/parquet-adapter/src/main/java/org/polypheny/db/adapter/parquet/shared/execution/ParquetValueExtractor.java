package org.polypheny.db.adapter.parquet.shared.execution;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.polypheny.db.type.entity.PolyValue;


public interface ParquetValueExtractor {

    PolyValue extractValue( Group group, int index, Type type );

}
