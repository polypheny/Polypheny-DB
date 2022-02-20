package org.polypheny.db.adapter.file;


import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;


@Slf4j
@AdapterProperties(
        name = "File",
        description = "An adapter that stores all data as files. It is especially suitable for multimedia collections.",
        usedModes = DeployMode.EMBEDDED)
public class FileStore extends DataStore {

    // Standards
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    /**
     * Hash function to use the hash of a primary key to name a file.
     * If you change this function, make sure to change the offset in the {@link FileStore#commitOrRollback} method!
     */
    @SuppressWarnings("UnstableApiUsage") // see https://stackoverflow.com/questions/53060907/is-it-safe-to-use-hashing-class-from-com-google-common-hash
    public static final HashFunction SHA = Hashing.sha256();

    @Getter
    private final File rootDir;
    private FileStoreSchema currentSchema;

    private final File WAL; // A folder containing the write ahead log


    public FileStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );
        PolyphenyHomeDirManager fileManager = PolyphenyHomeDirManager.getInstance();
        File adapterRoot = fileManager.registerNewFolder( "data/file-store" );

        rootDir = fileManager.registerNewFolder( adapterRoot, "store" + getAdapterId() );

        WAL = fileManager.registerNewFolder( rootDir, "WAL" );

        trxRecovery();
        setInformationPage();
    }


    private void setInformationPage() {
        InformationGroup infoGroup = new InformationGroup( informationPage, "Disk usage in GB" );
        informationGroups.add( infoGroup );
        final File root = rootDir.toPath().getRoot().toFile();
        final int base;
        if ( SystemUtils.IS_OS_MAC ) {
            base = 1000;
        } else {
            base = 1024;
        }
        Double[] diskUsage = new Double[]{
                (double) ((root.getTotalSpace() - root.getUsableSpace()) / (long) Math.pow( base, 3 )),
                (double) (root.getUsableSpace() / (long) Math.pow( base, 3 )) };
        InformationGraph infoElement = new InformationGraph(
                infoGroup,
                GraphType.DOUGHNUT,
                new String[]{ "used", "free" },
                new GraphData<>( "disk-usage", diskUsage ) );
        infoGroup.setRefreshFunction( () -> {
            Double[] updatedDiskUsage = new Double[]{
                    (double) ((root.getTotalSpace() - root.getUsableSpace()) / (long) Math.pow( base, 3 )),
                    (double) (root.getUsableSpace() / (long) Math.pow( base, 3 )) };
            infoElement.updateGraph( new String[]{ "used", "free" }, new GraphData<>( "disk-usage", updatedDiskUsage ) );
        } );
        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( infoGroup );
        im.registerInformation( infoElement );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        // it might be worth it to check why createNewSchema is called multiple times with different names
        if ( currentSchema == null ) {
            currentSchema = new FileStoreSchema( rootSchema, name, this );
        }
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createFileTable( catalogTable, columnPlacementsOnStore, partitionPlacement );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable, List<Long> partitionIds ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );

        for ( long partitionId : partitionIds ) {
            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionId,
                    "unused",
                    "unused" );

            for ( Long colId : catalogTable.columnIds ) {
                File newColumnFolder = getColumnFolder( colId, partitionId );
                if ( !newColumnFolder.mkdir() ) {
                    throw new RuntimeException( "Could not create column folder " + newColumnFolder.getAbsolutePath() );
                }
            }
        }

        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapterPerTable( getAdapterId(), catalogTable.id ) ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    placement.columnId,
                    "unused",
                    "unused",
                    true );
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable, List<Long> partitionIds ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        // TODO check if it is on this store?

        for ( long partitionId : partitionIds ) {
            catalog.deletePartitionPlacement( getAdapterId(), partitionId );
            for ( Long colId : catalogTable.columnIds ) {
                File f = getColumnFolder( colId, partitionId );
                try {
                    FileUtils.deleteDirectory( f );
                } catch ( IOException e ) {
                    throw new RuntimeException( "Could not drop table " + colId, e );
                }
            }
        }
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );

        CatalogColumnPlacement ccp = null;
        for ( CatalogColumnPlacement p : Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( getAdapterId(), catalogTable.id ) ) {
            // The for loop is required to avoid using the names of the column which we are currently adding (which are null)
            if ( p.columnId != catalogColumn.id ) {
                ccp = p;
                break;
            }
        }
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( ccp.adapterId, catalogTable.id ) ) {
            File newColumnFolder = getColumnFolder( catalogColumn.id, partitionPlacement.partitionId );
            if ( !newColumnFolder.mkdir() ) {
                throw new RuntimeException( "Could not create column folder " + newColumnFolder.getName() );
            }

            // Add default values
            if ( catalogColumn.defaultValue != null ) {
                try {
                    CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                    File primaryKeyDir = new File( rootDir, getPhysicalColumnName( primaryKey.columnIds.get( 0 ), partitionPlacement.partitionId ) );
                    for ( File entry : primaryKeyDir.listFiles() ) {
                        FileModifier.write( new File( newColumnFolder, entry.getName() ), catalogColumn.defaultValue.value );
                    }
                } catch ( IOException e ) {
                    throw new RuntimeException( "Caught exception while inserting default values", e );
                }
            }
        }

        catalog.updateColumnPlacementPhysicalNames(
                getAdapterId(),
                catalogColumn.id,
                currentSchema.getSchemaName(),
                "unused",
                false );
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );

        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( columnPlacement.adapterId, columnPlacement.tableId ) ) {
            File columnFile = getColumnFolder( columnPlacement.columnId, partitionPlacement.partitionId );
            try {
                FileUtils.deleteDirectory( columnFile );
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not delete column folder", e );
            }
        }
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        throw new RuntimeException( "File adapter does not support adding indexes" );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        throw new RuntimeException( "File adapter does not support dropping indexes" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        addWAL( xid, "commit" );
        commitOrRollback( xid, true );
        removeWAL( xid );
    }


    @Override
    public void rollback( PolyXid xid ) {
        addWAL( xid, "rollback" );
        commitOrRollback( xid, false );
        removeWAL( xid );
    }


    void addWAL( final PolyXid key, final String value ) {
        String fileName = SHA.hashString( key.toString(), CHARSET ).toString();
        File wal = new File( WAL, fileName );
        try ( PrintWriter pw = new PrintWriter( new FileWriter( wal ) ) ) {
            pw.println( Hex.encodeHexString( key.getGlobalTransactionId() ) );
            pw.println( Hex.encodeHexString( key.getBranchQualifier() ) );
            pw.println( value );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not add entry to WAL", e );
        }
    }


    void removeWAL( final PolyXid key ) {
        String fileName = SHA.hashString( key.toString(), CHARSET ).toString();
        File wal = new File( WAL, fileName );
        wal.delete();
    }


    /**
     * To recover from a crash, the file adapter checks if there is entries in the WAL folder
     * It will continue to execute the WAL entries
     */
    void trxRecovery() {
        File[] walFiles = WAL.listFiles( file -> !file.isHidden() );
        if ( walFiles == null ) {
            return;
        }
        try {
            for ( File f : walFiles ) {
                String GID;
                String BID;
                String action;
                try ( BufferedReader br = new BufferedReader( new FileReader( f ) ) ) {
                    GID = br.readLine();
                    BID = br.readLine();
                    action = br.readLine();
                }
                PolyXid xid = new PolyXid( Hex.decodeHex( GID ), Hex.decodeHex( BID ) );
                switch ( action ) {
                    case "commit":
                        commitOrRollback( xid, true );
                        break;
                    case "rollback":
                        commitOrRollback( xid, false );
                        break;
                    default:
                        throw new RuntimeException( "Unexpected WAL entry: " + action );
                }
                f.delete();
            }
        } catch ( IOException | DecoderException e ) {
            log.error( "Could not recover", e );
        }
    }


    public void commitOrRollback( final PolyXid xid, final boolean commit ) {
        String xidHash = SHA.hashString( xid.toString(), CHARSET ).toString();
        final String deletePrefix;
        final String movePrefix;
        if ( commit ) {
            deletePrefix = "_del_" + xidHash;
            movePrefix = "_ins_" + xidHash;
        } else {
            deletePrefix = "_ins_" + xidHash;
            movePrefix = "_del_" + xidHash;
        }
        if ( rootDir.listFiles() != null ) {
            for ( File columnFolder : rootDir.listFiles( File::isDirectory ) ) {
                for ( File data : columnFolder.listFiles( f -> !f.isHidden() && f.getName().startsWith( deletePrefix ) ) ) {
                    data.delete();
                }
                File data = null;
                File target = null;
                File[] fileList = columnFolder.listFiles( f -> !f.isHidden() && f.getName().startsWith( movePrefix ) );
                if ( fileList == null ) {
                    return;
                }
                try {
                    for ( File file : fileList ) {
                        data = file;
                        String hash = data.getName().substring( 70 );// 3 + 3 + 64 (three underlines + "ins" + xid hash)
                        target = new File( columnFolder, hash );
                        if ( commit ) {
                            Files.move( data.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING );
                        } else {
                            Files.move( data.toPath(), target.toPath() );
                        }
                    }
                } catch ( IOException e ) {
                    if ( target == null ) {
                        throw new RuntimeException( "Could not commit because moving of files failed", e );
                    } else {
                        throw new RuntimeException( "Could not commit because moving of files failed, trying to move "
                                + data.getAbsolutePath() + " to " + target.getAbsolutePath(), e );
                    }
                }
            }
        }
        cleanupHardlinks( xid );
    }


    private void cleanupHardlinks( final PolyXid xid ) {
        File hardlinkFolder = new File( rootDir, "hardlinks/" + SHA.hashString( xid.toString(), FileStore.CHARSET ).toString() );
        if ( hardlinkFolder.exists() ) {
            try {
                FileHelper.deleteDirRecursively( hardlinkFolder );
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not cleanup hardlink-folder " + hardlinkFolder.getAbsolutePath(), e );
            }
        }
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        //context.getStatement().getTransaction().registerInvolvedStore( this );
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( getAdapterId(), table.id ) ) {
            FileTranslatableTable fileTable = (FileTranslatableTable) currentSchema.getTable( table.name + "_" + partitionPlacement.partitionId );
            try {
                for ( String colName : fileTable.getColumnNames() ) {
                    File columnFolder = getColumnFolder( fileTable.getColumnIdMap().get( colName ), fileTable.getPartitionId() );
                    FileUtils.cleanDirectory( columnFolder );
                }
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not truncate file table", e );
            }
        }
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement placement, CatalogColumn catalogColumn, PolyType oldType ) {
        //context.getStatement().getTransaction().registerInvolvedStore( this );
        throw new RuntimeException( "File adapter does not support updating column types!" );
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return new ArrayList<>();
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        throw new RuntimeException( "File adapter does not support adding indexes" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        // TODO: Check if this is correct and ind better approach
        List<Long> pkIds = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey ).columnIds;
        return ImmutableList.of( new FunctionalIndexInfo( pkIds, "PRIMARY (unique)" ) );
    }


    @Override
    public void shutdown() {
        log.info( "Shutting down file store '{}'", getUniqueName() );
        removeInformationPage();
        try {
            FileHelper.deleteDirRecursively( rootDir );
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not delete all files from file store", e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        throw new UnsupportedOperationException( "Cannot change directory" );
    }


    protected static String getPhysicalColumnName( long columnId, long partitionId ) {
        return "col" + columnId + "_" + partitionId;
    }


    public static File getColumnFolder( final String rootPath, final long columnId, final long partitionId ) {
        File root = new File( rootPath );
        return new File( root, getPhysicalColumnName( columnId, partitionId ) );
    }


    public File getColumnFolder( final long columnId, final long partitionId ) {
        return new File( rootDir, getPhysicalColumnName( columnId, partitionId ) );
    }

}
