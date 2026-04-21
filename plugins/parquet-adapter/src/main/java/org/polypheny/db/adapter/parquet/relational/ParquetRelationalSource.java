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

package org.polypheny.db.adapter.parquet.relational;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.pf4j.Extension;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetNormalizedSchema;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSchemaMode;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSchemaNormalizer;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTableBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.io.ParquetUrlResolver;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.prepare.Context;

/**
 * Relational adapter implementation.
 * Manages exported table discovery, schema registration, information-page content,
 * and restore of relational Parquet tables.
 */
@Extension
@AdapterProperties(
        name = ParquetRelationalSource.NAME,
        description = "A relational adapter for querying Parquet files. The location of the directory containing the Parquet files can be specified. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "upload", "link", "url" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
@AdapterSettingList(name = ParquetRelationalSource.SCHEMA_MODE_SETTING, options = { "flat", "normalized" }, defaultValue = "flat", description = "Controls how Parquet schemas are exposed. Flat preserves the existing one-file-one-table import. Normalized will expose nested fields as generated relational tables.", position = 3)
@AdapterSettingDirectory(subOf = "method_upload", name = "directory", defaultValue = "classpath://orders_db", description = "You can upload one or multiple .parquet files.", position = 2)
@AdapterSettingString(subOf = "method_link", defaultValue = "classpath://orders_db", name = "directoryName", description = "You can select a path to a folder or specific .parquet files.", position = 2)
@AdapterSettingString(subOf = "method_url", defaultValue = "", name = "url", description = "URL to the Parquet file(s) to be integrated as this source.", position = 2)
public class ParquetRelationalSource extends AbstractParquetSource implements RelationalDataSource {

    public static final String NAME = "Parquet Relational";
    public static final String SCHEMA_MODE_SETTING = "schema mode";

    private ParquetSchemaMode schemaMode;
    private ParquetNormalizedSchema normalizedSchema;


    public ParquetRelationalSource( long storeId, String uniqueName, Map<String, String> settings, DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, Set.of( DataModel.RELATIONAL ) );
        this.schemaMode = getConfiguredSchemaMode();
    }


    @Override
    public RelationalDataSource asRelationalDataSource() {
        return this;
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        ParquetSchemaMode activeMode = schemaMode == null ? getConfiguredSchemaMode() : schemaMode;
        if ( activeMode == ParquetSchemaMode.NORMALIZED ) {
            return getNormalizedExportedColumns();
        }
        return super.getExportedColumns();
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.table.name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                logical.pkIds, allocation );

        ParquetTableBinding binding = getDiscoveredTableBinding( logical.table.name, table );
        registerParquetBinding( table.id, binding );
        var physical = currentNamespace.createParquetTable( table.id, table, binding, this );
        adapterCatalog.replacePhysical( physical );
        return List.of( physical );
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        PhysicalEntity table = entities.get( 0 );
        updateNamespace( table.namespaceName, table.namespaceId );
        PhysicalTable physicalTable = table.unwrapOrThrow( PhysicalTable.class );
        ParquetTableBinding binding = getParquetBinding( table.id ).orElseGet( () -> currentNamespace.createRootBinding( physicalTable ) );
        var physical = currentNamespace.createParquetTable( table.id, physicalTable, binding, this );
        adapterCatalog.addPhysical( alloc, physical );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        List<PhysicalEntity> physicals = adapterCatalog.getPhysicalsFromAllocs( allocId );
        if ( physicals != null ) {
            physicals.forEach( physical -> removeParquetBinding( physical.id ) );
        }
        adapterCatalog.removeAllocAndPhysical( allocId );
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, org.polypheny.db.catalog.entity.allocation.AllocationCollection allocation ) {
        log.debug( "NOT SUPPORTED: relational Parquet source does not support method createCollection()." );
        return null;
    }


    @Override
    public void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: relational Parquet source does not support method restoreCollection()." );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        log.debug( "NOT SUPPORTED: relational Parquet source does not support method dropCollection()." );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: Parquet source does not support method createGraph()." );
        return null;
    }


    @Override
    public void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: Parquet source does not support method restoreGraph()." );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: Parquet source does not support method dropGraph()." );
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        adapterCatalog.renameLogicalColumn( id, newColumnName );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        super.reloadSettings( updatedSettings );
        if ( updatedSettings.contains( "directory" ) || updatedSettings.contains( "directoryName" ) || updatedSettings.contains( "url" ) ) {
            clearNormalizedExportCache();
        }
        if ( updatedSettings.contains( SCHEMA_MODE_SETTING ) ) {
            schemaMode = getConfiguredSchemaMode();
            clearExportedColumnsCache();
            clearNormalizedExportCache();
        }
    }


    private ParquetSchemaMode getConfiguredSchemaMode() {
        String value = settings.getOrDefault( SCHEMA_MODE_SETTING, ParquetSchemaMode.FLAT.getSettingValue() );
        ParquetSchemaMode mode = ParquetSchemaMode.from( value );
        if ( mode == ParquetSchemaMode.FLAT && !ParquetSchemaMode.FLAT.getSettingValue().equalsIgnoreCase( value ) ) {
            log.warn( "Unknown Parquet schema mode '{}'. Falling back to flat mode.", value );
        }
        return mode;
    }


    private Map<String, List<ExportedColumn>> getNormalizedExportedColumns() {
        return getNormalizedSchema().getTables();
    }


    private ParquetTableBinding getDiscoveredTableBinding( String tableName, PhysicalTable table ) {
        ParquetNormalizedSchema normalizedSchema = getNormalizedSchema();
        DiscoveredTableBinding binding = normalizedSchema.getBinding( tableName );
        if ( binding == null && getConfiguredSchemaMode() == ParquetSchemaMode.NORMALIZED ) {
            clearNormalizedExportCache();
            normalizedSchema = getNormalizedSchema();
            binding = normalizedSchema.getBinding( tableName );
        }
        if ( binding == null ) {
            if ( getConfiguredSchemaMode() == ParquetSchemaMode.NORMALIZED ) {
                throw new GenericRuntimeException( "Missing normalized Parquet binding for generated table: %s", tableName );
            }
            return createFlatTableBinding( tableName, table );
        }
        return ParquetTableBinding.createTableBindingFromColumnPaths( binding.sourceUrl(), binding.parentTableName(), binding.sourcePathElements(), table, binding.columnPaths() );
    }


    private ParquetTableBinding createFlatTableBinding( String tableName, PhysicalTable table ) {
        List<ExportedColumn> exportedColumns = super.getExportedColumns().get( tableName );
        if ( exportedColumns == null || exportedColumns.isEmpty() ) {
            return currentNamespace.createRootBinding( table );
        }

        try {
            Map<String, List<String>> columnPaths = new LinkedHashMap<>();
            exportedColumns.forEach( column -> columnPaths.put( column.name(), List.of( column.physicalColumnName() ) ) );
            String sourceUrl = ParquetUrlResolver.resolveFile( parquetDir, exportedColumns.get( 0 ).physicalSchemaName() ).toString();
            return ParquetTableBinding.createTableBindingFromColumnPaths( sourceUrl, null, List.of(), table, columnPaths );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private void clearNormalizedExportCache() {
        normalizedSchema = null;
    }

    private ParquetNormalizedSchema getNormalizedSchema() {
        if ( connectionMethod == ConnectionMethod.UPLOAD && normalizedSchema != null ) {
            return normalizedSchema;
        }

        normalizedSchema = new ParquetSchemaNormalizer(
                parquetDir,
                getClass().getClassLoader(),
                parquetTypeConverter,
                getUniqueName() ).normalize();

        return normalizedSchema;
    }

}
