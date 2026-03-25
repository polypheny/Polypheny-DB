package org.polypheny.db.adapter.parquet.schema;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.adapter.parquet.execution.ParquetEnumerator;
import org.polypheny.db.adapter.parquet.model.ParquetFilter;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Scannable table
 */
public class ParquetScannableTable extends ParquetTable implements ScannableEntity {

    public ParquetScannableTable( long id, Source source, PhysicalTable table, java.util.List<PolyType> fieldTypes, int[] fields, ParquetSource parquetSource ) {
        super( id, source, table, fieldTypes, fields, parquetSource );
    }

    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        // create parquet enumerator
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                // empty filter list
                List<ParquetFilter> filterList = java.util.List.of();
                return new ParquetEnumerator( source, cancelFlag, fields, filterList );
            }
        };
    }
}
