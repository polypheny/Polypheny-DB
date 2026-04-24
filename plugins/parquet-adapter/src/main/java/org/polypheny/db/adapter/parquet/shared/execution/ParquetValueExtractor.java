package org.polypheny.db.adapter.parquet.shared.execution;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.polypheny.db.type.entity.PolyValue;
import java.util.List;

/**
 * Interface implemented by value extractors
 * that can convert a Parquet field into a `PolyValue`
 */
public interface ParquetValueExtractor {

    PolyValue extractValue( Group group, int index, Type type );

    PolyValue extractValue( Group group, List<String> path );

}
