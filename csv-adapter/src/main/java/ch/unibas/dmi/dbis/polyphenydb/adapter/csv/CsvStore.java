package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvTable.Flavor;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import java.io.File;


public class CsvStore implements Store {

    private static File csvDir = new File( "testTestCsv" );
    private final CatalogCombinedSchema combinedSchema;


    public CsvStore( CatalogCombinedSchema combinedSchema ) {
        this.combinedSchema = combinedSchema;
    }


    @Override
    public Schema getSchema( SchemaPlus rootSchema ) {
        return new CsvSchema( csvDir, Flavor.FILTERABLE, combinedSchema );
    }

}
