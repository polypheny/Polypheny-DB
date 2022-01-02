/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.StringJoiner;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PolyphenyHomeDirManager;


/**
 * A data source that can Query a File System
 */
@Slf4j
@AdapterProperties(
        name = "QFS",
        description = "This data source maps a file system on the Polypheny-DB host system as a relational table and allows to query it.",
        usedModes = DeployMode.EMBEDDED)
@AdapterSettingString(name = "rootDir", defaultValue = "")
public class Qfs extends DataSource {

    @Getter
    private File rootDir;
    private QfsSchema currentSchema;


    public Qfs( int adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, true );
        init( settings );
        registerInformationPage( uniqueName );
    }


    private void init( final Map<String, String> settings ) {
        rootDir = new File( settings.get( "rootDir" ) );
        if ( !rootDir.exists() ) {
            throw new RuntimeException( "The specified root dir does not exist!" );
        }
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new QfsSchema( rootSchema, name, this );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createFileTable( combinedTable, columnPlacementsOnStore, partitionPlacement );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "QFS does not support truncate" );
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
            throw new RuntimeException( "The specified QFS root dir does not exist!" );
        }
        boolean allowed = false;
        StringJoiner allowedPaths = new StringJoiner( "\n" );
        int numberOfWhitelistEntries = 0;
        File whitelistFolder = PolyphenyHomeDirManager.getInstance().registerNewFolder( "config" );
        File whitelist = new File( whitelistFolder, "whitelist.config" );
        String path = whitelist.getAbsolutePath();
        if ( !whitelist.exists() ) {
            try ( FileWriter fw = new FileWriter( whitelist ); PrintWriter pw = new PrintWriter( fw ) ) {
                pw.println( "# A list of allowed directories for the Query File System (QFS) data source adapter" );
                pw.println( "# The list must be non-empty. A QFS directory will only be accepted if it is listed here or is a subdirectory of a directory listed here." );
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not write QFS whitelist file " + path, e );
            }
            throw new RuntimeException( "The QFS whitelist did not exist. A new one was generated. Make sure to add at least one entry to the whitelist before deploying a QFS data source. The whitelist is located in " + path );
        }
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
            throw new RuntimeException( "Could not read QFS whitelist. A whitelist must be present and contain at least one entry. It must be located in " + path, e );
        }
        if ( numberOfWhitelistEntries == 0 ) {
            throw new RuntimeException( "The QFS whitelist must contain at least one entry. The file can be edited in " + path );
        }
        if ( !allowed ) {
            throw new RuntimeException( "The selected path (" + newSettings.get( "rootDir" ) + ") is not allowed. It must be a subdirectory of one of the following paths:\n" + allowedPaths.toString() );
        }
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
            im.registerInformation( table );
            informationElements.add( table );
        }
    }

}
