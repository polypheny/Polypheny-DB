package org.polypheny.db.adapter.file;


import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import org.polypheny.db.util.FileSystemManager;


@Slf4j
public class FileStore extends Store {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "File";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "An adapter that stores all data as files. It is especially suitable for multimedia collections.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of();

    @Getter
    private File rootDir;
    private FileSchema currentSchema;

    //Standards
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    /**
     * Hash function to use the hash of a primary key to name a file.
     * If you change this function, make sure to change the offset in the {@link FileStore#commitOrRollback} method!
     */
    @SuppressWarnings("UnstableApiUsage") // see https://stackoverflow.com/questions/53060907/is-it-safe-to-use-hashing-class-from-com-google-common-hash
    public static final HashFunction SHA = Hashing.sha256();


    public FileStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, false, false, true );
        setRootDir();
    }


    private void setRootDir() {
        File adapterRoot = FileSystemManager.getInstance().registerDataFolder( "file-store" );
        rootDir = new File( adapterRoot, "store" + getStoreId() );

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
                throw new RuntimeException( "Could not create column folder " + newColumnFolder.getAbsolutePath() );
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
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        String xidHash = SHA.hashString( xid.toString(), CHARSET ).toString();
        final String deletePrefix = "_del_" + xidHash;
        final String movePrefix = "_ins_" + xidHash;
        if ( rootDir.listFiles() != null ) {
            for ( File columnFolder : rootDir.listFiles( f -> f.isDirectory() ) ) {
                for ( File data : columnFolder.listFiles( f -> !f.isHidden() && f.getName().startsWith( deletePrefix ) ) ) {
                    data.delete();
                }
                try {
                    for ( File data : columnFolder.listFiles( f -> !f.isHidden() && f.getName().startsWith( movePrefix ) ) ) {
                        String hash = data.getName().substring( 70 );// 3 + 3 + 64 (three underlines + "ins" + xid hash)
                        File target = new File( columnFolder, hash );
                        if ( target.exists() ) {
                            //todo check
                            //throw new RuntimeException("Found a PK duplicate during commit");
                        }
                        //todo check REPLACE
                        Files.move( data.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING );
                    }
                } catch ( IOException e ) {
                    throw new RuntimeException( "Could not commit because moving of files failed", e );
                }
            }
        }
    }


    @Override
    public void rollback( PolyXid xid ) {
        String xidHash = SHA.hashString( xid.toString(), CHARSET ).toString();
        final String deletePrefix = "_del_" + xidHash;
        final String movePrefix = "_ins_" + xidHash;
        if ( rootDir.listFiles() != null ) {
            for ( File columnFolder : rootDir.listFiles( f -> f.isDirectory() ) ) {
                for ( File data : columnFolder.listFiles( f -> !f.isHidden() && f.getName().startsWith( movePrefix ) ) ) {
                    data.delete();
                }
                try {
                    for ( File data : columnFolder.listFiles( f -> !f.isHidden() && f.getName().startsWith( deletePrefix ) ) ) {
                        String hash = data.getName().substring( 70 );// 3 + 3 + 64 (three underlines + "ins" + xid hash)
                        File target = new File( columnFolder, hash );
                        /*if( target.exists() ) {
                            throw new RuntimeException("Found a PK duplicate during rollback");
                        }*/
                        Files.move( data.toPath(), target.toPath() );
                    }
                } catch ( IOException e ) {
                    throw new RuntimeException( "Could not commit because moving of files failed", e );
                }
            }
        }
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
        try {
            //from https://www.baeldung.com/java-delete-directory
            Files.walk( rootDir.toPath() )
                    .sorted( Comparator.reverseOrder() )
                    .map( Path::toFile )
                    .forEach( File::delete );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not delete all files from file store", e );
        }
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
