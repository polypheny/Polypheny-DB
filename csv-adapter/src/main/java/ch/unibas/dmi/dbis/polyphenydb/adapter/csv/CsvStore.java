package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.util.Source;
import ch.unibas.dmi.dbis.polyphenydb.util.Sources;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Map;


public class CsvStore implements Store {

    private static File csvDir = new File( "testTestCsv" );


    public CsvStore() {

    }


    @Override
    public Map<String, Table> getTables( SchemaPlus schemaPlus ) {
        return createTableMap();
    }


    private static String trim( String s, String suffix ) {
        String trimmed = trimOrNull( s, suffix );
        return trimmed != null ? trimmed : s;
    }


    private static String trimOrNull( String s, String suffix ) {
        return s.endsWith( suffix )
                ? s.substring( 0, s.length() - suffix.length() )
                : null;
    }


    private Map<String, Table> createTableMap() {
        // Look for files in the directory ending in ".csv", ".csv.gz", ".json", ".json.gz".
        final Source baseSource = Sources.of( csvDir );
        File[] files = csvDir.listFiles( ( dir, name ) -> {
            final String nameSansGz = trim( name, ".gz" );
            return nameSansGz.endsWith( ".csv" ) || nameSansGz.endsWith( ".json" );
        } );
        if ( files == null ) {
            System.out.println( "directory " + csvDir + " not found" );
            files = new File[0];
        }
        // Build a map from table name to table; each file becomes a table.
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for ( File file : files ) {
            Source source = Sources.of( file );
            Source sourceSansGz = source.trim( ".gz" );
            final Source sourceSansJson = sourceSansGz.trimOrNull( ".json" );
            if ( sourceSansJson != null ) {
                JsonTable table = new JsonTable( source );
                builder.put( sourceSansJson.relative( baseSource ).path(), table );
                continue;
            }
            final Source sourceSansCsv = sourceSansGz.trim( ".csv" );

            final Table table = new CsvFilterableTable( source, null );
            builder.put( sourceSansCsv.relative( baseSource ).path(), table );
        }
        return builder.build();
    }
}
