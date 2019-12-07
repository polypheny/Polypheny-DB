package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvTable.Flavor;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CsvStore extends Store {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "CSV";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "An adapter for querying CSV files.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "directory", false, true, true, "testTestCsv" )
    );

    private File csvDir;
    private CsvSchema currentSchema;


    public CsvStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings );
        csvDir = new File( settings.get( "directory" ) );
    }


    @Override
    public void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name ) {
        currentSchema = new CsvSchema( csvDir, Flavor.SCANNABLE );
    }


    @Override
    public Table createTableSchema( CatalogCombinedTable combinedTable ) {
        return currentSchema.createCsvTable( combinedTable );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogCombinedTable combinedTable ) {
        throw new RuntimeException( "CSV adapter does not support creating table" );
    }


    @Override
    public void dropTable( Context context, CatalogCombinedTable combinedTable ) {
        log.warn( "CSV adapter does not support dropping tables!" );
    }


    @Override
    public void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        log.warn( "CSV adapter does not support adding columns!" );
    }


    @Override
    public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        log.warn( "CSV adapter does not support dropping columns!" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.warn( "CSV Store does not support distributed transactions." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.warn( "CSV Store does not support distributed transactions." );
    }


    @Override
    public void truncate( Context context, CatalogCombinedTable table ) {
        log.warn( "CSV Store does not support truncate." );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumn catalogColumn ) {
        throw new RuntimeException( "CSV adapter does not support updating column types!" );
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
    public void updateSettings( Map<String, String> newSettings ) {
        for ( AdapterSetting s : AVAILABLE_SETTINGS ) {
            if ( newSettings.containsKey( s.name ) ) {
                if ( s.modifiable ) {
                    String newValue = newSettings.get( s.name );
                    if ( !s.canBeNull && newValue == null ) {
                        throw new RuntimeException( "Setting \"" + s.name + "\" is not allowed to be null!" );
                    }
                    if ( newValue.equals( settings.get( s.name ) ) ) {
                        //noinspection SwitchStatementWithTooFewBranches
                        switch ( s.name ) {
                            case "directory":
                                csvDir = new File( newValue );
                                break;

                            default:
                                throw new RuntimeException( "Missing entry for setting \"" + s.name + "\"!" );
                        }
                        settings.put( s.name, newValue );
                    } else {
                        // Same value, do nothing
                    }
                } else {
                    throw new RuntimeException( "Setting \"" + s.name + "\" is not modifiable!" );
                }
            }
        }
    }


    @Override
    public void shutdown() {
        // Nothing to do
    }

}
