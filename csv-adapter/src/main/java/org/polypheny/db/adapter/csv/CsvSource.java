package org.polypheny.db.adapter.csv;


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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingDirectory;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.csv.CsvTable.Flavor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;


@Slf4j
@AdapterProperties(
        name = "CSV",
        description = "An adapter for querying CSV files. The location of the directory containing the CSV files can be specified. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED)
@AdapterSettingDirectory(name = "directory", description = "You can upload one or multiple .csv or .csv.gz files.", position = 1)
@AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 2,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
public class CsvSource extends DataSource {

    private URL csvDir;
    private CsvSchema currentSchema;
    private final int maxStringLength;
    private Map<String, List<ExportedColumn>> exportedColumnCache;


    public CsvSource( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );

        // Validate maxStringLength setting
        maxStringLength = Integer.parseInt( settings.get( "maxStringLength" ) );
        if ( maxStringLength < 1 ) {
            throw new RuntimeException( "Invalid value for maxStringLength: " + maxStringLength );
        }

        setCsvDir( settings );
        addInformationExportedColumns();
        enableInformationPage();
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
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createCsvTable( catalogTable, columnPlacementsOnStore, this, partitionPlacement );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "CSV adapter does not support truncate" );
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        if ( exportedColumnCache != null ) {
            return exportedColumnCache;
        }
        Map<String, List<ExportedColumn>> exportedColumnCache = new HashMap<>();
        Set<String> fileNames;
        if ( csvDir.getProtocol().equals( "jar" ) ) {
            List<CatalogColumnPlacement> ccps = Catalog
                    .getInstance()
                    .getColumnPlacementsOnAdapter( getAdapterId() );
            fileNames = new HashSet<>();
            for ( CatalogColumnPlacement ccp : ccps ) {
                fileNames.add( ccp.physicalSchemaName );
            }
        } else {
            File[] files = Sources.of( csvDir )
                    .file()
                    .listFiles( ( d, name ) -> name.endsWith( ".csv" ) || name.endsWith( ".csv.gz" ) );
            fileNames = Arrays.stream( files )
                    .sequential()
                    .map( File::getName )
                    .collect( Collectors.toSet() );
        }
        for ( String fileName : fileNames ) {
            // Compute physical table name
            String physicalTableName = fileName.toLowerCase();
            if ( physicalTableName.endsWith( ".gz" ) ) {
                physicalTableName = physicalTableName.substring( 0, physicalTableName.length() - ".gz".length() );
            }
            physicalTableName = physicalTableName
                    .substring( 0, physicalTableName.length() - ".csv".length() )
                    .trim()
                    .replaceAll( "[^a-z0-9_]+", "" );

            List<ExportedColumn> list = new ArrayList<>();
            int position = 1;
            try {
                Source source = Sources.of( new URL( csvDir, fileName ) );
                BufferedReader reader = new BufferedReader( source.reader() );
                String firstLine = reader.readLine();
                for ( String col : firstLine.split( "," ) ) {
                    String[] colSplit = col.split( ":" );
                    String name = colSplit[0]
                            .toLowerCase()
                            .trim()
                            .replaceAll( "[^a-z0-9_]+", "" );
                    String typeStr = colSplit[1].toLowerCase().trim();
                    PolyType type;
                    PolyType collectionsType = null;
                    Integer length = null;
                    Integer scale = null;
                    Integer dimension = null;
                    Integer cardinality = null;
                    switch ( typeStr.toLowerCase() ) {
                        case "int":
                            type = PolyType.INTEGER;
                            break;
                        case "string":
                            type = PolyType.VARCHAR;
                            length = maxStringLength;
                            break;
                        case "boolean":
                            type = PolyType.BOOLEAN;
                            break;
                        case "long":
                            type = PolyType.BIGINT;
                            break;
                        case "float":
                            type = PolyType.REAL;
                            break;
                        case "double":
                            type = PolyType.DOUBLE;
                            break;
                        case "date":
                            type = PolyType.DATE;
                            break;
                        case "time":
                            type = PolyType.TIME;
                            length = 0;
                            break;
                        case "timestamp":
                            type = PolyType.TIMESTAMP;
                            length = 0;
                            break;
                        default:
                            throw new RuntimeException( "Unknown type: " + typeStr.toLowerCase() );
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
                            fileName,
                            physicalTableName,
                            name,
                            position,
                            position == 1 ) ); // TODO
                    position++;
                }
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

            exportedColumnCache.put( physicalTableName, list );
        }
        return exportedColumnCache;
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
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            setCsvDir( settings );
        }
    }


    private void addInformationExportedColumns() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup( informationPage, entry.getValue().get( 0 ).physicalSchemaName );
            informationGroups.add( group );

            InformationTable table = new InformationTable(
                    group,
                    Arrays.asList( "Position", "Column Name", "Type", "Nullable", "Filename", "Primary" ) );
            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                table.addRow(
                        exportedColumn.physicalPosition,
                        exportedColumn.name,
                        exportedColumn.getDisplayType(),
                        exportedColumn.nullable ? "✔" : "",
                        exportedColumn.physicalSchemaName,
                        exportedColumn.primary ? "✔" : ""
                );
            }
            informationElements.add( table );
        }
    }

}
