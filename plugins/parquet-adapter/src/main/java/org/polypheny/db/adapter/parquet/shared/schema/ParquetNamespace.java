package org.polypheny.db.adapter.parquet.shared.schema;

import java.net.MalformedURLException;
import java.net.URL;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.io.ParquetUrlResolver;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;

/**
 * Create wrappers for parquet files
 * either a relational table or a document collection.
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


    public ParquetTableBinding createRootBinding( PhysicalTable table ) {
        Source parquetSource = Sources.of( ParquetUrlResolver.resolveFile( directoryUrl, table.name + ".parquet" ) );
        return ParquetTableBinding.createRootTableBinding( parquetSource.url().toString(), table );
    }

    /**
     * Creates a Parquet table wrapper for a physical table entry.
     */
    public ParquetRelTable createParquetTable( long id, PhysicalTable table, ParquetTableBinding binding, AbstractParquetSource sourceAdapter ) {
        try {
            // source file comes from the binding, not from the generated table name
            Source parquetSource = Sources.of( new URL( binding.sourceUrl() ) );
            return new ParquetRelTable( id, parquetSource, table, binding, sourceAdapter );

        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public ParquetDocument createParquetCollection( PhysicalCollection collection, AbstractParquetSource sourceAdapter ) {
        Source parquetSource = Sources.of( ParquetUrlResolver.resolveFile( directoryUrl, collection.name + ".parquet" ) );
        return new ParquetDocument( collection, parquetSource, sourceAdapter );
    }


    @Override
    protected @Nullable Convention getConvention() {
        return null;
    }

}
