package org.polypheny.db.adapter.multimedia;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;


@Slf4j
public class MultimediaStore extends Store {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "MULTIMEDIA";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "An adapter for multimedia files. All data is saved as files.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "directory", false, true, false, "testTestMM" )
    );

    @Getter
    private File rootDir;
    private MultimediaSchema currentSchema;
    public final static String COLUMN_FILE_EXTENSION = ".txt";


    public MultimediaStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, false, false, true );
        setRootDir( settings );
    }


    private void setRootDir( Map<String, String> settings ) {
        String dir = settings.get( "directory" );
        //if ( dir.startsWith( "classpath://" ) )

        rootDir = new File( dir );
        if( !rootDir.exists() ) {
            if( !rootDir.mkdir() ) {
                throw new RuntimeException( "Could not create root directory" );
            }
        }
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new MultimediaSchema( name, this );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createMultimediaTable( catalogTable, columnPlacementsOnStore );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        try {
            for( CatalogColumnPlacement placement: catalog.getColumnPlacementsOnStore( getStoreId(), catalogTable.id )) {
                catalog.updateColumnPlacementPhysicalNames( getStoreId(), placement.columnId, currentSchema.getSchemaName(), getPhysicalTableName( catalogTable.id ), getPhysicalColumnName( placement.columnId ) );
            }
        } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
            throw new RuntimeException( "Could not create table", e );
        }
        for( Long colId: catalogTable.columnIds ) {
            File newColumnFolder = getColumnFolder( colId );
            if( !newColumnFolder.mkdir() ) {
                throw new RuntimeException( "Could not create column file" );
            }
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable ) {
        //todo check if it is on this store?
        for( Long colId: catalogTable.columnIds ) {
            File f = getColumnFolder( colId );
            try {
                FileUtils.deleteDirectory( f );
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not drop multimedia table " + colId, e );
            }
        }
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        File newColumnFolder = getColumnFolder( catalogColumn.id );
        if( !newColumnFolder.mkdir() ) {
            throw new RuntimeException( "Could not create column file" );
        }
        try {
            catalog.updateColumnPlacementPhysicalNames(
                    getStoreId(),
                    catalogColumn.id,
                    currentSchema.getSchemaName(),
                    getPhysicalTableName(  catalogTable.id),
                    getPhysicalColumnName( catalogColumn.id ));
        } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        File columnFile = getColumnFolder( columnPlacement.columnId );
        try {
            FileUtils.deleteDirectory( columnFile );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not delete multimedia column file", e );
        }
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "MM Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "MM Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "MM Store does not support rollback()." );
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        log.warn( "MM Store does not support truncate." );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement placement, CatalogColumn catalogColumn ) {
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
    public void shutdown() {
        log.info( "shutting down MM store '{}'", getUniqueName() );
        try {
            //from https://www.baeldung.com/java-delete-directory
            Files.walk( rootDir.toPath() )
                    .sorted( Comparator.reverseOrder() )
                    .map( Path::toFile )
                    .forEach( File::delete );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not delete all files from MM store", e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // todo move all files to new destination
        throw new UnsupportedOperationException( "Cannot change directory" );
    }

    protected String getPhysicalTableName( long tableId ) {
        return "tab" + tableId;
    }


    protected String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }

    public File getColumnFolder ( final Long columnId ) {
        return new File( rootDir, getPhysicalColumnName( columnId ) );
    }

}
