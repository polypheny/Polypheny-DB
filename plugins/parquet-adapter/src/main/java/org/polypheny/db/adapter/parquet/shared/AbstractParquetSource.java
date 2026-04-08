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

package org.polypheny.db.adapter.parquet.shared;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.Scannable;
import org.polypheny.db.adapter.parquet.shared.io.ParquetFileDiscovery;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNamespace;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetFieldNameNormalizer;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for both source types.
 * Handles settings, file discovery, exported schema derivation, information-page setup,
 * name normalization, and shared restore behavior.
 */
public abstract class AbstractParquetSource extends DataSource<DocAdapterCatalog> implements Scannable {

    protected final ConnectionMethod connectionMethod;
    protected final ParquetTypeConverter parquetTypeConverter;
    protected URL parquetDir;

    protected ParquetNamespace currentNamespace;
    private Map<String, List<ExportedColumn>> exportedColumns;

    protected static final Logger log = LoggerFactory.getLogger( AbstractParquetSource.class );


    protected AbstractParquetSource( long storeId, String uniqueName, Map<String, String> settings, DeployMode mode, Set<DataModel> supportedModels ) {
        super( storeId, uniqueName, settings, mode, true, new DocAdapterCatalog( storeId ), supportedModels );
        this.parquetTypeConverter = new ParquetTypeConverter();
        this.connectionMethod = settings.containsKey( "method" )
                ? ConnectionMethod.from( settings.get( "method" ).toUpperCase() )
                : ConnectionMethod.UPLOAD;
        setParquetDir( settings );
        createInformationPage();
        enableInformationPage();
    }


    protected void setParquetDir( Map<String, String> settings ) {
        switch ( connectionMethod ) {
            case LINK -> {
                String dir = settings.get( "directoryName" );
                if ( dir.startsWith( "classpath://" ) ) {
                    parquetDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
                } else {
                    try {
                        parquetDir = new File( dir ).toURI().toURL();
                    } catch ( MalformedURLException e ) {
                        throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
                    }
                }
            }
            case UPLOAD -> {
                String dir = settings.get( "directory" );
                if ( dir.startsWith( "classpath://" ) ) {
                    parquetDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
                } else {
                    try {
                        parquetDir = new File( dir ).toURI().toURL();
                    } catch ( MalformedURLException e ) {
                        throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
                    }
                }
            }
            case URL -> {
                String dir = settings.get( "url" );
                try {
                    parquetDir = new URL( dir );
                } catch ( MalformedURLException e ) {
                    throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
                }
            }
        }
    }


    protected static String getValidTableName( String name ) {
        return name.trim().replaceAll( "[^a-z0-9_]+", "" );
    }


    protected static String computePhysicalTableName( String fileName ) {
        String physicalTableName = fileName.toLowerCase();
        if ( physicalTableName.endsWith( ".parquet" ) ) {
            physicalTableName = physicalTableName.substring( 0, physicalTableName.length() - ".parquet".length() );
        }
        return getValidTableName( physicalTableName );
    }


    public static String normalizeFieldName( String name ) {
        return ParquetFieldNameNormalizer.normalizeFieldName( name );
    }


    protected String getValidColumnNameFromField( Type field ) {
        return normalizeFieldName( field.getName() );
    }


    private List<ExportedColumn> getExportedColumnsFromFile( String fileName, String physicalTableName ) {
        try {
            Path path = new Path( new URL( parquetDir, fileName ).toURI() );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );
            try ( ParquetFileReader reader = ParquetFileReader.open( HadoopInputFile.fromPath( path, conf ) ) ) {
                List<Type> schemaFields = reader.getFooter().getFileMetaData().getSchema().getFields();
                List<ExportedColumn> columns = new ArrayList<>();
                int position = 0;
                for ( Type field : schemaFields ) {
                    columns.add( getExportedColumnFromField( field, fileName, physicalTableName, position++ ) );
                }
                return columns;
            }
        } catch ( Exception e ) {
            throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
        }
    }


    private ExportedColumn getExportedColumnFromField( Type field, String fileName, String physicalTableName, int position ) {
        String columnName = getValidColumnNameFromField( field );
        PolyType polyType = parquetTypeConverter.fromParquetTypeToPolyType( field );
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


    private void createInformationPage() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            if ( entry.getValue().isEmpty() ) {
                continue;
            }
            InformationGroup group = new InformationGroup( informationPage, entry.getValue().get( 0 ).physicalSchemaName() );
            informationGroups.add( group );
            informationElements.add( getInformationTable( group, entry ) );
        }
    }


    private InformationTable getInformationTable( InformationGroup group, Map.Entry<String, List<ExportedColumn>> entry ) {
        List<String> columns = Arrays.asList( "Position", "Column Name", "Type", "Nullable", "Filename", "Primary" );
        InformationTable table = new InformationTable( group, columns );
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


    @Override
    public void updateNamespace( String name, long id ) {
        currentNamespace = new ParquetNamespace( id, adapterId, parquetDir );
        putNamespace( currentNamespace );
    }


    @Override
    public ParquetNamespace getCurrentNamespace() {
        return currentNamespace;
    }


    @Override
    public AdapterCatalog getCatalog() {
        return adapterCatalog;
    }


    @Override
    public void truncate( Context context, long allocId ) {
        throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( "Parquet adapter does not support truncate" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Parquet Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "Parquet Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Parquet Store does not support rollback()." );
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) || updatedSettings.contains( "directoryName" ) ) {
            setParquetDir( settings );
        }
    }

}
