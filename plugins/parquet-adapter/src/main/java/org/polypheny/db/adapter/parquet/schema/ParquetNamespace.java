package org.polypheny.db.adapter.parquet.schema;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;

/**
 * Create wrappers for parquet files
 */
public class ParquetNamespace extends Namespace {

    private final URL directoryUrl;


    /**
     * Constructor:
     * Creates a namespace bound to a directory URL and table flavor.
     */
    public ParquetNamespace( long id, long adapterId, URL directoryUrl ) {
        super( id, adapterId );
        this.directoryUrl = directoryUrl;
    }


    /**
     * Creates a Parquet table wrapper for a physical table entry.
     */
    public ParquetTable createParquetTable( long id, PhysicalTable table, ParquetSource sourceAdapter ) {
        List<PolyType> fieldTypes = new ArrayList<>();
        List<Integer> fieldIds = new ArrayList<>( table.columns.size() );

        List<PhysicalColumn> columns = table.getColumns();

        for ( int i = 0; i < columns.size(); i++ ) {
            PhysicalColumn column = columns.get( i );
            fieldTypes.add( column.type );
            fieldIds.add( i );
        }

        Source parquetSource;
        try {
            parquetSource = Sources.of( new URL( directoryUrl, table.name + ".parquet" ) );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }

        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();
        return createTable( id, parquetSource, table, fieldTypes, fields, sourceAdapter );
    }


    /**
     * Instantiates the table class based on configured flavor.
     */
    private ParquetTable createTable( long id, Source source, PhysicalTable table, List<PolyType> fieldTypes, int[] fields, ParquetSource parquetSource ) {
        return new ParquetTable( id, source, table, fieldTypes, fields, parquetSource );
    }


    @Override
    protected @Nullable Convention getConvention() {
        return null;
    }

}
