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

package org.polypheny.db.adapter.file.source;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;


/**
 * A data source that can query a file system
 */
@Slf4j
@AdapterProperties(
        name = "QFS",
        description = "This data source maps a file system on the Polypheny-DB host system as a relational entity and allows to query it.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingString(name = "rootDir", defaultValue = "")
public class Qfs extends DataSource<RelAdapterCatalog> {

    @Delegate(excludes = Exclude.class)
    private final RelationalScanDelegate delegate;

    @Getter
    private File rootDir;

    @Getter
    private QfsSchema currentNamespace;


    public Qfs( long adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, true, new RelAdapterCatalog( adapterId ) );
        init( settings );
        registerInformationPage( uniqueName );
        this.delegate = new RelationalScanDelegate( this, adapterCatalog );
    }


    private void init( final Map<String, String> settings ) {
        rootDir = new File( settings.get( "rootDir" ) );
        if ( !rootDir.exists() ) {
            throw new GenericRuntimeException( "The specified root dir does not exist!" );
        }
    }


    @Override
    public void updateNamespace( String name, long id ) {
        currentNamespace = new QfsSchema( id, adapterId, name, this );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        //Todo
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.getTable().name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                logical.pkIds, allocation );
        FileTranslatableEntity physical = currentNamespace.createFileTable( table );
        adapterCatalog.replacePhysical( physical );
        return List.of( physical );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        throw new NotImplementedException();//Todo
    }


    @Override
    public void truncate( Context context, long allocId ) {
        throw new GenericRuntimeException( "QFS does not support truncate" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "QFS does not support truncate" );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "QFS does not support commit" );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "QFS does not support rollback" );
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        init( settings );
        InformationManager im = InformationManager.getInstance();
        im.getInformation( getUniqueName() + "-rootDir" ).unwrap( InformationText.class ).setText( settings.get( "rootDir" ) );
    }


    @Override
    protected void validateSettings( Map<String, String> newSettings, boolean initialSetup ) {
        super.validateSettings( newSettings, initialSetup );
        File rootDir = new File( newSettings.get( "rootDir" ) );
        if ( !rootDir.exists() ) {
            throw new GenericRuntimeException( "The specified QFS root dir does not exist!" );
        }
        boolean allowed = false;
        StringJoiner allowedPaths = new StringJoiner( "\n" );
        int numberOfWhitelistEntries = 0;
        File whitelistFolder = PolyphenyHomeDirManager.getInstance().registerNewFolder( "config" );
        File whitelist = new File( whitelistFolder, "whitelist.config" );
        String path = getString( whitelist );
        try ( FileInputStream fis = new FileInputStream( whitelist ); BufferedReader br = new BufferedReader( new InputStreamReader( fis ) ) ) {
            String line;
            while ( (line = br.readLine()) != null ) {
                line = line.trim();
                if ( line.startsWith( "#" ) ) {
                    continue;
                }
                File f = new File( line );
                if ( !f.exists() ) {
                    log.warn( "The following QFS whitelist entry does not exist: {}", line );
                    continue;
                }
                numberOfWhitelistEntries++;
                allowedPaths.add( f.getCanonicalPath() );
                if ( rootDir.getCanonicalPath().startsWith( f.getCanonicalPath() ) ) {
                    allowed = true;
                    break;
                }
            }
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not read QFS whitelist. A whitelist must be present and contain at least one entry. It must be located in " + path, e );
        }
        if ( numberOfWhitelistEntries == 0 ) {
            throw new GenericRuntimeException( "The QFS whitelist must contain at least one entry. The file can be edited in " + path );
        }
        if ( !allowed ) {
            throw new GenericRuntimeException( "The selected path (" + newSettings.get( "rootDir" ) + ") is not allowed. It must be a subdirectory of one of the following paths:\n" + allowedPaths.toString() );
        }
    }


    @NotNull
    private static String getString( File whitelist ) {
        String path = whitelist.getAbsolutePath();
        if ( !whitelist.exists() ) {
            try ( FileWriter fw = new FileWriter( whitelist ); PrintWriter pw = new PrintWriter( fw ) ) {
                pw.println( "# A list of allowed directories for the Query File System (QFS) data source adapter" );
                pw.println( "# The list must be non-empty. A QFS directory will only be accepted if it is listed here or is a subdirectory of a directory listed here." );
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Could not write QFS whitelist file " + path, e );
            }
            throw new GenericRuntimeException( "The QFS whitelist did not exist. A new one was generated. Make sure to add at least one entry to the whitelist before deploying a QFS data source. The whitelist is located in " + path );
        }
        return path;
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        //name, extension, path, mime, canExecute, canRead, canWrite, size, lastModified
        String physSchemaName = getUniqueName();
        String physTableName = getUniqueName();
        List<ExportedColumn> columns = new ArrayList<>();

        columns.add( new ExportedColumn(
                "path",
                PolyType.VARCHAR,
                null,
                1000,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "path",
                1,
                true
        ) );

        columns.add( new ExportedColumn(
                "name",
                PolyType.VARCHAR,
                null,
                500,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "name",
                2,
                false
        ) );

        columns.add( new ExportedColumn(
                "size",
                PolyType.BIGINT,
                null,
                null,
                null,
                null,
                null,
                true,
                physSchemaName,
                physTableName,
                "size",
                3,
                false
        ) );

        columns.add( new ExportedColumn(
                "file",
                PolyType.FILE,
                null,
                null,
                null,
                null,
                null,
                false,
                physSchemaName,
                physTableName,
                "file",
                4,
                false
        ) );

        Map<String, List<ExportedColumn>> out = new HashMap<>();
        out.put( getUniqueName(), columns );
        return out;
    }


    protected void registerInformationPage( String uniqueName ) {
        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );

        InformationGroup rootGroup = new InformationGroup( informationPage, "Root directory" ).setOrder( 1 );
        InformationText iText = new InformationText( uniqueName + "-rootDir", rootGroup, settings.get( "rootDir" ) );
        im.addGroup( rootGroup );
        im.registerInformation( iText );

        int i = 2;
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup( informationPage, entry.getValue().get( 0 ).physicalTableName ).setOrder( i++ );
            im.addGroup( group );
            informationGroups.add( group );

            InformationTable table = getInformationTable( entry, group );
            im.registerInformation( table );
            informationElements.add( table );
        }
    }


    @NotNull
    private static InformationTable getInformationTable( Entry<String, List<ExportedColumn>> entry, InformationGroup group ) {
        InformationTable table = new InformationTable(
                group,
                Arrays.asList( "Position", "Column Name", "Type", "Nullable", "Primary" ) );
        for ( ExportedColumn exportedColumn : entry.getValue() ) {
            table.addRow(
                    exportedColumn.physicalPosition,
                    exportedColumn.name,
                    exportedColumn.getDisplayType(),
                    exportedColumn.nullable ? "✔" : "",
                    exportedColumn.primary ? "✔" : ""
            );
        }
        return table;
    }


    @SuppressWarnings("UnnecessaryModifier")
    public static interface Exclude {

        @SuppressWarnings("unused")
        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

        @SuppressWarnings("unused")
        List<PhysicalEntity> refreshTable( long allocId );

        @SuppressWarnings("unused")
        void dropTable( Context context, long allocId );

    }

}
