package org.polypheny.db.adapter.csv;


import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.csv.CsvTable.Flavor;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;


@Slf4j
public class CsvSource extends DataSource {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "CSV";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "An adapter for querying CSV files. The location of the directory containing the CSV files can be specified. Currently, this adapter only supports read operations.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "directory", false, true, true, "test" )
    );

    private URL csvDir;
    private CsvSchema currentSchema;


    public CsvSource( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );
        setCsvDir( settings );
    }


    private void setCsvDir( Map<String, String> settings ) {
        String dir = settings.get( "directory" );
        if ( dir.startsWith( "classpath://" ) ) {
            csvDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
        } else {
            try {
                csvDir = new File( dir ).toURI().toURL();
            } catch ( MalformedURLException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new CsvSchema( csvDir, Flavor.SCANNABLE );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createCsvTable( catalogTable, columnPlacementsOnStore, this );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        Map<String, List<ExportedColumn>> map = new HashMap<>();
        Set<String> fileNames;
        if ( csvDir.getProtocol().equals( "jar" ) ) {
            Reflections reflections = new Reflections( "hr", new ResourcesScanner() );
            Set<String> fileNamesSet = reflections.getResources( Pattern.compile( ".*\\.csv" ) );
            fileNames = new HashSet<>();
            for ( String fileName : fileNamesSet ) {
                String[] fileNameSplit = fileName.split( "/" );
                fileNames.add( fileNameSplit[fileNameSplit.length - 1] );
            }
        } else {
            fileNames = Arrays.stream( Sources.of( csvDir )
                    .file()
                    .listFiles( ( d, name ) -> name.endsWith( ".csv" ) ) )
                    .sequential()
                    .map( File::getName )
                    .collect( Collectors.toSet() );
        }
        for ( String fileName : fileNames ) {
            List<ExportedColumn> list = new ArrayList<>();
            int position = 1;
            try {
                Source source = Sources.of( new URL( csvDir, fileName ) );
                BufferedReader reader = new BufferedReader( source.reader() );
                String firstLine = reader.readLine();
                for ( String col : firstLine.split( "," ) ) {
                    String[] colSplit = col.split( ":" );
                    String name = colSplit[0].toLowerCase().trim();
                    String typeStr = colSplit[1].toLowerCase().trim();
                    PolyType type;
                    PolyType collectionsType;
                    Integer length;
                    Integer scale;
                    Integer dimension;
                    Integer cardinality;
                    switch ( typeStr ) {
                        case "int":
                            type = PolyType.INTEGER;
                            collectionsType = null;
                            length = null;
                            scale = null;
                            dimension = null;
                            cardinality = null;
                            break;
                        case "string":
                            type = PolyType.VARCHAR;
                            collectionsType = null;
                            length = 255; // TODO
                            scale = null;
                            dimension = null;
                            cardinality = null;
                            break;
                        default:
                            throw new RuntimeException( "Unknown type: " + typeStr );
                    }
                    list.add( new ExportedColumn(
                            name,
                            type,
                            collectionsType,
                            length,
                            scale,
                            dimension,
                            cardinality,
                            false,
                            null,
                            fileName.substring( 0, fileName.length() - 4 ),
                            name,
                            position,
                            position == 1 ) ); // TODO
                    position++;
                }
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
            map.put( fileName.substring( 0, fileName.length() - 4 ), list );
        }
        return map;
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "CSV Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "CSV Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "CSV Store does not support rollback()." );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        // Nothing to do
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            setCsvDir( settings );
        }
    }

}
