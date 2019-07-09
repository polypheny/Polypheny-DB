package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvTable.Flavor;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import java.io.File;


public class CsvStore implements Store {

    private static File csvDir = new File( "testTestCsv" );
    private final CsvSchema schema;


    public CsvStore() {
        schema = new CsvSchema( csvDir, Flavor.FILTERABLE );
    }


    @Override
    public void addDatabase( String databaseName ) {

    }


    @Override
    public void addSchema( String schemaName, String databaseName ) {

    }


    @Override
    public void addTable( String tableName, String schemaName, String databaseName ) {

    }


    @Override
    public Schema getSchema() {
        return schema;
    }
}
