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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.Scannable;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTable;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTableBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetBindingSerializer;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.io.ParquetFileDiscovery;
import org.polypheny.db.adapter.parquet.shared.io.ParquetUrlResolver;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNamespace;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for both source types.
 * Handles settings, file discovery, exported schema derivation, information-page setup,
 * name normalization, and shared restore behavior.
 */
public abstract class AbstractParquetSource extends DataSource<DocAdapterCatalog> implements Scannable {

    protected static final Logger log = LoggerFactory.getLogger( AbstractParquetSource.class );
    private static final String PARQUET_BINDINGS_SETTING = "__polypheny_parquet_bindings";
    protected final ConnectionMethod connectionMethod;
    private final Map<Long, ParquetTableBinding> parquetBindings;
    protected URL parquetDir;
    protected ParquetNamespace currentNamespace;
    private Map<String, DiscoveredTable> discoveredTables;


    protected AbstractParquetSource( long storeId, String uniqueName, Map<String, String> settings, DeployMode mode, Set<DataModel> supportedModels ) {
        super( storeId, uniqueName, settings, mode, true, new DocAdapterCatalog( storeId ), supportedModels );
        // Recover binding metadata through adapter settings
        this.parquetBindings = new HashMap<>( ParquetBindingSerializer.deserialize( settings.get( PARQUET_BINDINGS_SETTING ) ) );
        this.connectionMethod = settings.containsKey( "method" )
                ? ConnectionMethod.from( settings.get( "method" ).toUpperCase() )
                : ConnectionMethod.UPLOAD;
        setParquetDir( settings );
        createInformationPage();
        enableInformationPage();
    }


    /**
     * populate parquet directory variable according to settings
     *
     * @param settings Settings Map
     */
    protected void setParquetDir( Map<String, String> settings ) {
        switch ( connectionMethod ) {
            case LINK -> {
                String dir = settings.get( "directoryName" );
                if ( dir.startsWith( "classpath://" ) ) {
                    parquetDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
                } else {
                    try {
                        parquetDir = ParquetUrlResolver.asSourceUrl( new File( dir ).toURI().toURL() );
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
                        parquetDir = ParquetUrlResolver.asSourceUrl( new File( dir ).toURI().toURL() );
                    } catch ( MalformedURLException e ) {
                        throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
                    }
                }
            }
            case URL -> {
                String dir = settings.get( "url" );
                try {
                    parquetDir = ParquetUrlResolver.asSourceUrl( new URL( dir ) );
                } catch ( MalformedURLException e ) {
                    throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
                }
            }
        }
    }


    /**
     * Return exported column map if exists
     * Otherwise creates hashmap:
     * for each filename - store exported columns list
     *
     * @return Map<String, List<ExportedColumn>>
     */
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        if ( discoveredTables == null ) {
            discoveredTables = ParquetFileDiscovery.discoverTables( parquetDir, getUniqueName() );
        }
        return discoveredTables.values().stream().collect( LinkedHashMap::new, ( m, t ) -> m.put( t.tableName(), t.columns() ), HashMap::putAll );
    }


    protected Optional<DiscoveredTableBinding> getTableBinding( String tableName ) {
        if ( discoveredTables == null ) {
            discoveredTables = ParquetFileDiscovery.discoverTables( parquetDir, getUniqueName() );
        }
        return Optional.ofNullable( discoveredTables.get( tableName ).binding() );
    }


    /**
     * Allows ParquetRelationalSource to clear the flat exported-column cache when settings such as
     * schemaMode or directory change.
     */
    protected void clearTablesCache() {
        discoveredTables = null;
    }


    /**
     * Add binding to the parquet bindings map and call for persistence to store map in settings
     *
     * @param physicalTableId - key in Map<Long, ParquetTableBinding>
     * @param binding ParquetTableBinding to store
     */
    protected void registerParquetBinding( long physicalTableId, ParquetTableBinding binding ) {
        // for this physical table id, remember this Parquet binding
        parquetBindings.put( physicalTableId, binding );
        persistParquetBindings();
    }


    protected Optional<ParquetTableBinding> getParquetBinding( long physicalTableId ) {
        return Optional.ofNullable( parquetBindings.get( physicalTableId ) );
    }


    protected void removeParquetBinding( long physicalTableId ) {
        if ( parquetBindings.remove( physicalTableId ) != null ) {
            persistParquetBindings();
        }
    }


    /**
     * Store parquet bindings map in settings
     */
    private void persistParquetBindings() {
        settings.put( PARQUET_BINDINGS_SETTING, ParquetBindingSerializer.serialize( parquetBindings ) );
        Catalog.getInstance().updateAdapterSettings( adapterId, new HashMap<>( settings ) );
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
        if ( updatedSettings.contains( "directory" ) || updatedSettings.contains( "directoryName" ) || updatedSettings.contains( "url" ) ) {
            setParquetDir( settings );
            clearTablesCache();
        }
    }

}
