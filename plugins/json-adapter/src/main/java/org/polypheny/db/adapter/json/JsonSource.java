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

package org.polypheny.db.adapter.json;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.experimental.Delegate;
import org.pf4j.Extension;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.transaction.PolyXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
@AdapterProperties(
        name = "JSON",
        description = "An adapter for querying JSON files. A json file can be specified by path. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "upload", "link" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
@AdapterSettingDirectory(subOf = "method_upload", name = "directory", defaultValue = "classpath://hr", description = "You can upload one or multiple .csv or .csv.gz files.", position = 2)
@AdapterSettingString(subOf = "method_link", defaultValue = "classpath://hr", name = "directoryName", description = "You can select a path to a folder or specific .csv or .csv.gz files.", position = 2)
@AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 3,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
public class JsonSource extends DataSource<DocAdapterCatalog> {

    private static final Logger log = LoggerFactory.getLogger( JsonSource.class );
    @Delegate(excludes = Excludes.class)
    private final DocumentScanDelegate delegate;
    private final ConnectionMethod connectionMethod;

    private URL jsonFile;
    @Getter

    private Map<String, List<ExportedColumn>> exportedColumnCache;


    public JsonSource( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true, new DocAdapterCatalog( storeId ) );
        this.connectionMethod = settings.containsKey( "method" ) ? ConnectionMethod.from( settings.get( "method" ).toUpperCase() ) : ConnectionMethod.UPLOAD;
        this.jsonFile = getJsonFileUrl( settings );

        //addInformationExportedColumns();
        //enableInformationPage();

        this.delegate = new DocumentScanDelegate( this, adapterCatalog );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            this.jsonFile = getJsonFileUrl( settings );
        }
    }


    private URL getJsonFileUrl( Map<String, String> settings ) {
        String dir = settings.get( "file" );
        if ( dir.startsWith( "classpath://" ) ) {
            return this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
        }
        try {
            return new File( dir ).toURI().toURL();
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "JSON store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "JSON store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "JSON store does not support rollback()." );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        throw new GenericRuntimeException( "JSON store does not support truncate" );
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    public void updateNamespace( String name, long id ) {
    }


    @Override
    public Namespace getCurrentNamespace() {
        return null;
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        return null;
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        return null;
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        return null;
    }


    private static String computePhysicalEntityName( String fileName ) {
        // Compute physical table name
        String physicalTableName = fileName.toLowerCase();
        // remove gz
        if ( physicalTableName.endsWith( ".gz" ) ) {
            physicalTableName = physicalTableName.substring( 0, physicalTableName.length() - ".gz".length() );
        }
        // use only filename
        if ( physicalTableName.contains( "/" ) ) {
            String[] splits = physicalTableName.split( "/" );
            physicalTableName = splits[splits.length - 2];
        }

        if ( physicalTableName.contains( "\\" ) ) {
            String[] splits = physicalTableName.split( "\\\\" );
            physicalTableName = splits[splits.length - 2];
        }

        return physicalTableName
                .substring( 0, physicalTableName.length() - ".csv".length() )
                .trim()
                .replaceAll( "[^a-z0-9_]+", "" );
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


    protected void updateNativePhysical( long allocId ) {

    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        adapterCatalog.renameLogicalColumn( id, newColumnName );
        adapterCatalog.fields.values().stream().filter( c -> c.id == id ).forEach( c -> updateNativePhysical( c.allocId ) );
    }


    @SuppressWarnings("unused")
    private interface Excludes {

        void renameLogicalColumn( long id, String newColumnName );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

        void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities );

    }

}
