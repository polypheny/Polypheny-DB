/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet;

import lombok.Getter;
import lombok.experimental.Delegate;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.Type;

import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource;
import org.polypheny.db.adapter.RelationalScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.parquet.io.ParquetFileDiscovery;
import org.polypheny.db.adapter.parquet.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.schema.ParquetNamespace;
import org.polypheny.db.adapter.parquet.schema.ParquetTable;


import org.polypheny.db.adapter.parquet.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.pf4j.Extension;
import org.polypheny.db.type.PolyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
@AdapterProperties(
        name = "Parquet",
        description = "An adapter for querying Parquet files. The location of the directory containing the Parquet files can be specified. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "upload", "link" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
@AdapterSettingDirectory(subOf = "method_upload", name = "directory", defaultValue = "classpath://orders_db", description = "You can upload one or multiple .parquet files.", position = 2)
@AdapterSettingString(subOf = "method_link", defaultValue = "classpath://orders_db", name = "directoryName", description = "You can select a path to a folder or specific .parquet files.", position = 2)
public class ParquetSource extends DataSource<RelAdapterCatalog> implements RelationalDataSource {

    @Delegate(excludes = Excludes.class)
    private final RelationalScanDelegate delegate;

    private final ConnectionMethod connectionMethod;
    //private final int maxStringLength;
    private final ParquetTypeConverter parquetTypeConverter;
    private URL parquetDir;

    @Getter
    private ParquetNamespace currentNamespace;
    private Map<String, List<ExportedColumn>> exportedColumns;

    private static final Logger log = LoggerFactory.getLogger( ParquetSource.class );


    /**
     * Constructor
     * Creates the Parquet source from adapter settings.
     */
    public ParquetSource(final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, true, new RelAdapterCatalog( storeId ), Set.of( DataModel.RELATIONAL ) );

        this.parquetTypeConverter = new ParquetTypeConverter();
        this.connectionMethod = settings.containsKey( "method" )
                ? ConnectionMethod.from( settings.get( "method" ).toUpperCase() )
                : ConnectionMethod.UPLOAD;

        setParquetDir( settings );
        createInformationPage();
        enableInformationPage();

        this.delegate = new RelationalScanDelegate( this, adapterCatalog );
    }

    /**
     * Get directory information from settings and populate parquetDir variable
     */
    private void setParquetDir( Map<String, String> settings ) {
        String dir = settings.getOrDefault( "directory", "." );
        if ( connectionMethod == ConnectionMethod.LINK ) {
            dir = settings.getOrDefault( "directoryName", "." );
        }

        if ( dir.startsWith( "classpath://" ) ) {
            parquetDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
        } else {
            try {
                parquetDir = new File( dir ).toURI().toURL();
            } catch ( MalformedURLException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }

    /**
     * Removes invalid characters and creates a valid table name
     */
    private static String getValidTableName(String name){
        return name.trim().replaceAll( "[^a-z0-9_]+", "" );
    }


    /**
     * Build valid physical table name from file name
     */
    private static String computePhysicalTableName( String fileName ) {
        String physicalTableName = fileName.toLowerCase();
        if ( physicalTableName.endsWith( ".parquet" ) ) {
            physicalTableName = physicalTableName.substring( 0, physicalTableName.length() - ".parquet".length() );
        }
        return getValidTableName(physicalTableName);
    }


    /**
     * Build valid column name from field data
     */
    private String getValidColumnNameFromField(Type field) {
        return field.getName().toLowerCase().trim().replaceAll( "[^a-z0-9_]+", "_" );
    }

    /**
     * Get parquet file fields information from metadata
     */
    private List<ExportedColumn> getExportedColumnsFromFile( String fileName, String physicalTableName ) {
        try {
            Path path = new Path(new URL( parquetDir, fileName ).toURI());
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );
            try ( ParquetFileReader reader = ParquetFileReader.open( HadoopInputFile.fromPath(path, conf))) {
                List<Type> schemaFields = reader.getFooter().getFileMetaData().getSchema().getFields();
                List<ExportedColumn> columns = new ArrayList<>();
                int position = 0;
                for ( Type field : schemaFields ) {
                    ExportedColumn column = getExportedColumnFromField(field, fileName, physicalTableName, position);
                    columns.add(column);
                    position++;
                }
                return columns;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    /**
     * Extract metadata information from single parquet field and create column object
     */
    private ExportedColumn getExportedColumnFromField(Type field, String fileName, String physicalTableName, int position) {
        String columnName = getValidColumnNameFromField(field);
        PolyType polyType = parquetTypeConverter.fromParquetTypeToPolyType( field );
        //Integer length = polyType == PolyType.VARCHAR ? maxStringLength : null;
        return new ExportedColumn(
                    columnName,
                    polyType,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    fileName,
                    physicalTableName,
                    field.getName(),
                    position,
                    position == 0 );
    }

    /**
     * Create information page containing exported columns details
     * for all tables loaded from parquet files
     */
    private void createInformationPage() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            if ( entry.getValue().isEmpty() ) {
                continue;
            }

            InformationGroup group = new InformationGroup( informationPage, entry.getValue().get( 0 ).physicalSchemaName() );
            informationGroups.add( group );
            InformationTable table = getInformationTable(group, entry);
            informationElements.add( table );
        }
    }

    /**
     * Extract columns from all parquet files
     * @return Map<String, List<ExportedColumn>> - column list per table name
     */
    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        if ( connectionMethod == ConnectionMethod.UPLOAD && exportedColumns != null ) {
            return exportedColumns;
        }

        Map<String, List<ExportedColumn>> columns = new HashMap<>();
        Set<String> fileNames = ParquetFileDiscovery.listParquetFiles( parquetDir );

        for ( String fileName : fileNames ) {
            String physicalTableName = computePhysicalTableName( fileName );
            columns.put( physicalTableName, getExportedColumnsFromFile( fileName, physicalTableName ) );
        }

        this.exportedColumns = columns;
        return columns;

    }


    /**
     * Builds InformationTable from exported columns
     */
    private InformationTable getInformationTable(InformationGroup group, Map.Entry<String, List<ExportedColumn>> entry) {
        List<String> columns = Arrays.asList("Position", "Column Name", "Type", "Nullable", "Filename", "Primary");
        InformationTable table = new InformationTable(group, columns);
        for ( ExportedColumn exportedColumn : entry.getValue() ) {
            table.addRow(
                    exportedColumn.physicalPosition(),
                    exportedColumn.name(),
                    exportedColumn.getDisplayType(),
                    exportedColumn.nullable() ? "✔" : "",
                    exportedColumn.physicalSchemaName(),
                    exportedColumn.primary() ? "✔" : "" );
        }
        return table;
    }

    /**
     * Updates the active namespace wrapper for this source.
     */
    @Override
    public void updateNamespace( String name, long id ) {
        currentNamespace = new ParquetNamespace( id, adapterId, parquetDir, ParquetTable.Flavor.FILTERABLE );
    }

    /**
     * Returns this source as relational adapter interface.
     */
    @Override
    public RelationalDataSource asRelationalDataSource() {
        return this;
    }


    /**
     * Parquet source is read-only, so truncate is not supported.
     */
    @Override
    public void truncate( Context context, long allocId ) {
        throw new GenericRuntimeException( "Parquet adapter does not support truncate" );
    }

    /**
     * Prepare does nothing, because data source is read-only.
     */
    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Parquet Store does not support prepare()." );
        return true;
    }

    /**
     * Do nothing - read only
     */
    @Override
    public void commit( PolyXid xid ) {
        log.debug( "Parquet Store does not support commit()." );
        // do nothing
    }

    /**
     * Do nothing - read only
     */
    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Parquet Store does not support rollback()." );
        // do nothing
    }

    /**
     * Cleans up information page state.
     */
    @Override
    public void shutdown() {
        removeInformationPage();
    }

    /**
     * Reloads settings that affect source location.
     */
    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) || updatedSettings.contains( "directoryName" ) ) {
            setParquetDir( settings );
        }
    }

    //region Not Delegated Methods

    /**
     * Creates and registers a physical Parquet table entry.
     */
    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.table.name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                logical.pkIds, allocation );

        ParquetTable physical = currentNamespace.createParquetTable( table.id, table, this );
        adapterCatalog.replacePhysical( physical );
        return List.of( physical );
    }


    /**
     * Restores table metadata after restart
     */
    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        PhysicalEntity table = entities.get( 0 );
        updateNamespace( table.namespaceName, table.namespaceId );
        ParquetTable physical = currentNamespace.createParquetTable( table.id, table.unwrapOrThrow( PhysicalTable.class ), this );
        adapterCatalog.addPhysical( alloc,physical );
    }


    /**
     * Updates logical column naming in adapter catalog
     */
    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        adapterCatalog.renameLogicalColumn( id, newColumnName );
    }
    //endregion
    /**
     * Methods excluded from Lombok delegate forwarding.
     * These methods should not be delegated.
     */
    @SuppressWarnings("unused")
    private interface Excludes {

        void renameLogicalColumn( long id, String newColumnName );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

        void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context );

    }
}
