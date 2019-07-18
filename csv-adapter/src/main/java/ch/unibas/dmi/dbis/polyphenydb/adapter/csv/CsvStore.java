package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvTable.Flavor;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import java.io.File;


public class CsvStore implements Store {

    private static File csvDir = new File( "testTestCsv" );
    private CsvSchema schema;


    public CsvStore() {
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        schema = new CsvSchema( csvDir, Flavor.FILTERABLE );
    }


    @Override
    public Table createTableSchema( CatalogCombinedTable combinedTable ) {
        return schema.createCsvTable( combinedTable );
    }
}
