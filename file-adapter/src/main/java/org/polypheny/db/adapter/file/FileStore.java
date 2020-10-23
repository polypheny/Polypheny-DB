package org.polypheny.db.adapter.file;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
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
public class FileStore extends Store {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "FILE";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "An adapter that stores all data as files. It is especially suitable for multimedia collections.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of();

    @Getter
    private File rootDir;
    private FileSchema currentSchema;


    public FileStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, false, false, true );
        setRootDir();
    }


    private void setRootDir() {
        rootDir = new File( System.getProperty( "user.home" ), ".polypheny/file-adapter/store" + getStoreId() );
        if ( !rootDir.exists() ) {
            if ( !rootDir.mkdirs() ) {
                throw new RuntimeException( "Could not create root directory" );
            }
        }
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new FileSchema( rootSchema, name, this );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createFileTable( catalogTable, columnPlacementsOnStore );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        context.getStatement().getTransaction().registerInvolvedStore( this );
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
                throw new RuntimeException( "Could not create column folder" );
            }
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable ) {
        context.getStatement().getTransaction().registerInvolvedStore( this );
        //todo check if it is on this store?
        for( Long colId: catalogTable.columnIds ) {
            File f = getColumnFolder( colId );
            try {
                FileUtils.deleteDirectory( f );
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not drop table " + colId, e );
            }
        }
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        context.getStatement().getTransaction().registerInvolvedStore( this );
        File newColumnFolder = getColumnFolder( catalogColumn.id );
        if( !newColumnFolder.mkdir() ) {
            throw new RuntimeException( "Could not create column folder" );
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
        context.getStatement().getTransaction().registerInvolvedStore( this );
        File columnFile = getColumnFolder( columnPlacement.columnId );
        try {
            FileUtils.deleteDirectory( columnFile );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not delete column folder", e );
        }
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "File Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "File Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "File Store does not support rollback()." );
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        //context.getStatement().getTransaction().registerInvolvedStore( this );
        FileTranslatableTable fileTable = (FileTranslatableTable) currentSchema.getTable( table.name );
        try {
            for ( String colName : fileTable.getColumnNames() ) {
                File columnFolder = getColumnFolder( fileTable.columnIdMap.get( colName ) );
                FileUtils.cleanDirectory( columnFolder );
            }
        } catch ( IOException e ) {
            log.error( "Could not truncate file table", e );
        }
        //todo trx support
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement placement, CatalogColumn catalogColumn ) {
        //context.getStatement().getTransaction().registerInvolvedStore( this );
        throw new RuntimeException( "File adapter does not support updating column types!" );
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
        log.info( "shutting down file store '{}'", getUniqueName() );
        //delete only if it is empty (don't delete in case two stores have the same rootDir)
        rootDir.delete();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        throw new UnsupportedOperationException( "Cannot change directory" );
    }

    protected static String getPhysicalTableName( long tableId ) {
        return "tab" + tableId;
    }


    protected static String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }

    public static File getColumnFolder( final String rootPath, final Long columnId ) {
        File root = new File( rootPath );
        return new File( root, getPhysicalColumnName( columnId ) );
    }

    public File getColumnFolder( final Long columnId ) {
        return new File( rootDir, getPhysicalColumnName( columnId ) );
    }

}
