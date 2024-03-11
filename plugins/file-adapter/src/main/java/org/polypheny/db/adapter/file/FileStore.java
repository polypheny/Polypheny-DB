/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.pf4j.Extension;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalModifyDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
@Extension
@AdapterProperties(
        name = "File",
        description = "An adapter that stores all data as files. It is especially suitable for multimedia collections.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
public class FileStore extends DataStore<RelAdapterCatalog> {

    @Delegate(excludes = Exclude.class)
    private final RelationalModifyDelegate delegate;

    // Standards
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    /**
     * Hash function to use the hash of a primary key to name a file.
     * If you change this function, make sure to change the offset in the {@link FileStore#commitOrRollback} method!
     */
    public static final HashFunction SHA = Hashing.sha256();

    @Getter
    private final File rootDir;
    @Getter
    private FileStoreSchema currentNamespace;

    private final File WAL; // A folder containing the write ahead log

    @Getter
    private final List<PolyType> unsupportedTypes = ImmutableList.of( PolyType.ARRAY, PolyType.MAP );


    public FileStore( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true, new RelAdapterCatalog( storeId ) );
        PolyphenyHomeDirManager fileManager = PolyphenyHomeDirManager.getInstance();
        File adapterRoot = fileManager.registerNewFolder( "data/file-store" );

        rootDir = fileManager.registerNewFolder( adapterRoot, "store" + getAdapterId() );

        WAL = fileManager.registerNewFolder( rootDir, "WAL" );

        trxRecovery();
        setInformationPage();

        this.delegate = new RelationalModifyDelegate( this, adapterCatalog );
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
    public void updateNamespace( String name, long id ) {
        if ( currentNamespace == null ) {
            currentNamespace = new FileStoreSchema( id, adapterId, name, this );
        }

        putNamespace( currentNamespace );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        String physicalTableName = getPhysicalTableName( allocationWrapper.table.id );

        updateNamespace( logical.table.getNamespaceName(), logical.table.namespaceId );

        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                physicalTableName,
                allocationWrapper.columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> getPhysicalColumnName( c.columnId, allocationWrapper.table.id ) ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                logical.pkIds, allocationWrapper );

        for ( LogicalColumn col : logical.columns ) {
            File newColumnFolder = getColumnFolder( col.id, allocationWrapper.table.id );
            if ( !newColumnFolder.mkdir() ) {
                throw new GenericRuntimeException( "Could not create column folder " + newColumnFolder.getAbsolutePath() );
            }
        }

        FileTranslatableEntity physical = currentNamespace.createFileTable( table, logical.pkIds );

        adapterCatalog.replacePhysical( physical );
        return List.of( physical );
    }


    private String getPhysicalTableName( long id ) {
        return "tab" + id;
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        // TODO check if it is on this store?

        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        for ( long colId : table.getColumnIds() ) {
            File f = getColumnFolder( colId, allocId );
            try {
                FileUtils.deleteDirectory( f );
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Could not drop table " + colId, e );
            }
        }

        adapterCatalog.removeAllocAndPhysical( allocId );

    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        long allocId = adapterCatalog.fields.values().stream().filter( c -> c.id == id ).map( c -> c.allocId ).findFirst().orElseThrow();
        FileTranslatableEntity table = adapterCatalog.fromAllocation( allocId ).unwrap( FileTranslatableEntity.class ).orElseThrow();

        adapterCatalog.renameLogicalColumn( id, newColumnName );

        adapterCatalog.fields.values().stream().filter( c -> c.id == id ).forEach( c -> updateNativePhysical( c.allocId, table.getPkIds() ) );

        updateNativePhysical( allocId, table.getPkIds() );
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn logicalColumn ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );

        FileTranslatableEntity table = adapterCatalog.fromAllocation( allocId ).unwrap( FileTranslatableEntity.class ).orElseThrow();
        int max = adapterCatalog.getColumns( allocId ).stream().max( Comparator.comparingInt( a -> a.position ) ).orElseThrow().position;
        PhysicalColumn column = adapterCatalog.addColumn( getPhysicalColumnName( logicalColumn.id, allocId ), allocId, max + 1, logicalColumn );

        File newColumnFolder = getColumnFolder( column.id, allocId );
        if ( !newColumnFolder.mkdir() ) {
            throw new GenericRuntimeException( "Could not create column folder " + newColumnFolder.getName() );
        }

        PolyValue value = PolyNull.NULL;
        // Add default values
        if ( column.defaultValue != null ) {
            value = column.defaultValue.value;
        }
        try {
            File primaryKeyDir = new File( rootDir, getPhysicalColumnName( table.columns.get( 0 ).id, allocId ) );
            for ( File entry : Objects.requireNonNull( primaryKeyDir.listFiles() ) ) {
                FileModifier.write( new File( newColumnFolder, entry.getName() ), value );
            }
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Caught exception while inserting default values", e );
        }

        updateNativePhysical( allocId, table.getPkIds() );

    }


    protected void updateNativePhysical( long allocId, List<Long> pkIds ) {
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        adapterCatalog.replacePhysical( this.currentNamespace.createFileTable( table, pkIds ) );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        FileTranslatableEntity fileTranslatableEntity = adapterCatalog.fromAllocation( allocId ).unwrap( FileTranslatableEntity.class ).orElseThrow();

        adapterCatalog.dropColumn( allocId, columnId );
        File columnFile = getColumnFolder( columnId, allocId );
        try {
            FileUtils.deleteDirectory( columnFile );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not delete column folder", e );
        }

        updateNativePhysical( allocId, fileTranslatableEntity.getPkIds() );

    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        throw new GenericRuntimeException( "File adapter does not support adding indexes" );
    }


    @Override
    public void dropIndex( Context context, LogicalIndex catalogIndex, long allocId ) {
        throw new GenericRuntimeException( "File adapter does not support dropping indexes" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        addWAL( xid, TerminateAction.COMMIT );
        commitOrRollback( xid, true );
        removeWAL( xid );
    }


    @Override
    public void rollback( PolyXid xid ) {
        addWAL( xid, TerminateAction.ROLLBACK );
        commitOrRollback( xid, false );
        removeWAL( xid );
    }


    void addWAL( final PolyXid key, final TerminateAction action ) {
        String fileName = SHA.hashString( key.toString(), CHARSET ).toString();
        File wal = new File( WAL, fileName );
        try ( PrintWriter pw = new PrintWriter( new FileWriter( wal ) ) ) {
            pw.println( Hex.encodeHexString( key.getGlobalTransactionId() ) );
            pw.println( Hex.encodeHexString( key.getBranchQualifier() ) );
            pw.println( action.name() );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not add entry to WAL", e );
        }
    }


    void removeWAL( final PolyXid key ) {
        String fileName = SHA.hashString( key.toString(), CHARSET ).toString();
        File wal = new File( WAL, fileName );
        //noinspection ResultOfMethodCallIgnored
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
                TerminateAction action;
                try ( BufferedReader br = new BufferedReader( new FileReader( f ) ) ) {
                    GID = br.readLine();
                    BID = br.readLine();
                    action = TerminateAction.valueOf( br.readLine() );
                }
                PolyXid xid = new PolyXid( Hex.decodeHex( GID ), Hex.decodeHex( BID ) );
                switch ( action ) {
                    case COMMIT:
                        commitOrRollback( xid, true );
                        break;
                    case ROLLBACK:
                        commitOrRollback( xid, false );
                        break;
                    default:
                        throw new GenericRuntimeException( "Unexpected WAL entry: " + action );
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
                            if ( target.exists() ) {
                                throw new GenericRuntimeException( "Could not commit uniqueness constraint was invalidated" );
                            }
                            Files.move( data.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING );
                        } else {
                            Files.move( data.toPath(), target.toPath() );
                        }
                    }
                } catch ( IOException e ) {
                    if ( target == null ) {
                        throw new GenericRuntimeException( "Could not commit because moving of files failed", e );
                    } else {
                        throw new GenericRuntimeException( "Could not commit because moving of files failed, trying to move "
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
                throw new GenericRuntimeException( "Could not cleanup hardlink-folder " + hardlinkFolder.getAbsolutePath(), e );
            }
        }
    }


    @Override
    public void truncate( Context context, long allocId ) {
        //context.getStatement().getTransaction().registerInvolvedStore( this );
        PhysicalTable table = adapterCatalog.fromAllocation( allocId ).unwrap( FileTranslatableEntity.class ).orElseThrow();
        try {
            for ( PhysicalColumn column : table.columns ) {
                File columnFolder = getColumnFolder( column.id, allocId );
                FileUtils.cleanDirectory( columnFolder );
            }
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not truncate file table", e );
        }

    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
        //context.getStatement().getTransaction().registerInvolvedStore( this );
        throw new GenericRuntimeException( "File adapter does not support updating column types!" );
    }


    @Override
    public List<IndexMethodModel> getAvailableIndexMethods() {
        return ImmutableList.of();
    }


    @Override
    public IndexMethodModel getDefaultIndexMethod() {
        throw new GenericRuntimeException( "File adapter does not support adding indexes" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable ) {
        // TODO: Check if this is correct and ind better approach
        List<Long> pkIds = Catalog.snapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow().fieldIds;
        return ImmutableList.of( new FunctionalIndexInfo( pkIds, "PRIMARY (unique)" ) );
    }


    @Override
    public void shutdown() {
        log.info( "Shutting down file store '{}'", getUniqueName() );
        removeInformationPage();
        try {
            FileHelper.deleteDirRecursively( rootDir );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not delete all files from file store", e );
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


    private enum TerminateAction {
        COMMIT, ROLLBACK
    }


    @SuppressWarnings("unused")
    public interface Exclude {

        void renameLogicalColumn( long id, String name );

        void dropIndex( Context context, LogicalIndex catalogIndex, long allocId );

        String addIndex( Context context, LogicalIndex index, AllocationTable allocation );

        void dropColumn( Context context, long allocId, long columnId );

        void dropTable( Context context, long allocId );

        void updateColumnType( Context context, long allocId, LogicalColumn newCol );

        void addColumn( Context context, long allocId, LogicalColumn logicalColumn );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper );

    }

}
