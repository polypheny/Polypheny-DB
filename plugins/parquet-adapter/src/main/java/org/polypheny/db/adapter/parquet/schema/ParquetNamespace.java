package org.polypheny.db.adapter.parquet.schema;

import java.net.MalformedURLException;
import java.net.URL;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;
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
        try {
            Source parquetSource = Sources.of( new URL( directoryUrl, table.name + ".parquet" ) );
            return new ParquetTable( id, parquetSource, table, sourceAdapter );

        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public ParquetDocument createParquetCollection( PhysicalCollection collection, ParquetSource sourceAdapter ) {
        try {
            Source parquetSource = Sources.of( new URL( directoryUrl, collection.name + ".parquet" ) );
            return new ParquetDocument( collection, parquetSource, sourceAdapter );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    protected @Nullable Convention getConvention() {
        return null;
    }

}
