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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.pf4j.Extension;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTableBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetNormalizedSchema;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSchemaMode;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSchemaNormalizer;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetScan;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.rules.FilterSetOpTransposeRule;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPartitionGroup;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;

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
    public AlgNode getRelScan( long allocId, AlgBuilder builder ) {
        // This rule is still required for filter movement before the adapter rules inspect the tree.
        builder.getCluster().getPlanner().addRuleDuringRuntime( FilterSetOpTransposeRule.INSTANCE );
//        ParquetConvention.INSTANCE.register( builder.getCluster().getPlanner() );
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        ParquetRelTable table = entity.unwrapOrThrow( ParquetRelTable.class );
        return new ParquetScan( builder.getCluster(), table, IntStream.range( 0, table.getFieldCount() ).toArray() );
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
        Optional<String> partitionColumn = firstPolyphenyPartitionColumn( binding );
        // check if partitions exist
        if ( partitionColumn.isPresent() && binding.parentTableName() == null ) {
            return createPartitionedTable( logical, allocation, table, binding, partitionColumn.get() );
        }

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
            clearTablesCache();
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


    /**
     * converts DiscoveredTableBinding into ParquetTableBinding
     *
     * @param tableName - generated table name
     * @param table - physical table
     * @return ParquetTableBinding - column bindings are stored by physical column id
     */
    private ParquetTableBinding getDiscoveredTableBinding( String tableName, PhysicalTable table ) {
        // Get or build normalized schema
        ParquetNormalizedSchema normalizedSchema = getNormalizedSchema();
        // find binding for this table
        DiscoveredTableBinding binding = normalizedSchema.getBinding( tableName );
        // if missing in normalized mode - rebuild
        if ( binding == null && getConfiguredSchemaMode() == ParquetSchemaMode.NORMALIZED ) {
            clearNormalizedExportCache();
            normalizedSchema = getNormalizedSchema();
            binding = normalizedSchema.getBinding( tableName );
        }
        if ( binding == null ) {
            if ( getConfiguredSchemaMode() == ParquetSchemaMode.NORMALIZED ) {
                throw new GenericRuntimeException( "Missing normalized Parquet binding for generated table: %s", tableName );
            }
            // Flat tables do not have DiscoveredTableBinding,
            // because they come from AbstractParquetSource.getExportedColumns() not ParquetSchemaNormalizer
            return createFlatTableBinding( tableName, table );
        }
        // Convert discovered bindings to final ParquetTableBinding, where column bindings are stored by physical column id
        return ParquetTableBinding.createTableBindingFromColumnPaths( binding.sourceFiles(), binding.parentTableName(), binding.sourcePathElements(), table, binding.columnPaths() );
    }


    /**
     * Creates a ParquetTableBinding for a flat-mode table.
     * Flat-mode tables do not come from ParquetSchemaNormalizer, so they do not have a DiscoveredTableBinding.
     * But we still want a persisted ParquetTableBinding for them so restore/scanning uses the same binding infrastructure.
     *
     * @param tableName - generated table name
     * @param table - physical table
     * @return - ParquetTableBinding
     */
    private ParquetTableBinding createFlatTableBinding( String tableName, PhysicalTable table ) {
        // flat mode is implemented in AbstractParquetSource
        List<ExportedColumn> exportedColumns = super.getExportedColumns().get( tableName );
        if ( exportedColumns == null || exportedColumns.isEmpty() ) {
            return currentNamespace.createRootBinding( table );
        }

        try {
            DiscoveredTableBinding binding = getTableBinding( tableName ).orElseThrow();
            return ParquetTableBinding.createTableBindingFromColumnPaths( binding.sourceFiles(), null, List.of(), table, binding.columnPaths() );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private Optional<String> firstPolyphenyPartitionColumn( ParquetTableBinding binding ) {
        return binding.sourceFiles().stream()
                .flatMap( sourceFile -> sourceFile.partitionValues().keySet().stream() )
                .distinct()
                .filter( partitionColumn -> binding.columnsByColumnId().values().stream().anyMatch( column -> column.columnName().equals( partitionColumn ) ) )
                .findFirst();
    }


    /**
     * Creates physical entity (relational table) and adds top level partition to catalog.
     *
     * @param logical logical table
     * @param originalAllocation allocation table
     * @param originalPhysical physical table
     * @param binding table binding
     * @param partitionColumn partition column
     * @return list of tables if parquet file contains nested data.
     */
    private List<PhysicalEntity> createPartitionedTable(
            LogicalTableWrapper logical,
            AllocationTableWrapper originalAllocation,
            PhysicalTable originalPhysical,
            ParquetTableBinding binding,
            String partitionColumn ) {
        // group files by top level partitions
        Map<String, List<ParquetSourceFile>> filesByPartitionValue = binding.sourceFiles().stream()
                .filter( sourceFile -> sourceFile.partitionValues().containsKey( partitionColumn ) )
                .collect( Collectors.groupingBy(
                        sourceFile -> sourceFile.partitionValues().get( partitionColumn ),
                        LinkedHashMap::new,
                        Collectors.toList() ) );

        // one partition -> one physical table
        if ( filesByPartitionValue.size() <= 1 ) {
            registerParquetBinding( originalPhysical.id, binding );
            var physical = currentNamespace.createParquetTable( originalPhysical.id, originalPhysical, binding, this );
            adapterCatalog.replacePhysical( physical );
            return List.of( physical );
        }

        Catalog catalog = Catalog.getInstance();
        AllocationRelationalCatalog allocationCatalog = catalog.getAllocRel( logical.table.namespaceId );

        // Delete all temporary created allocations by polypheny first
        allocationCatalog.deleteAllocation( originalAllocation.table.id );
        allocationCatalog.deletePartition( originalAllocation.table.partitionId );
        Catalog.snapshot()
                .alloc()
                .getPartitionProperty( logical.table.id )
                .ifPresent( property -> property.partitionGroupIds.forEach( allocationCatalog::deletePartitionGroup ) );

        adapterCatalog.removeAllocAndPhysical( originalAllocation.table.id );


        LogicalColumn partitionLogicalColumn = logical.columns.stream()
                .filter( column -> column.name.equals( partitionColumn ) )
                .findFirst()
                .orElseThrow( () -> new GenericRuntimeException( "Missing parquet partition column in logical table: %s", partitionColumn ) );

        List<PhysicalEntity> physicals = new ArrayList<>();
        List<Long> partitionGroupIds = new ArrayList<>();
        List<Long> partitionIds = new ArrayList<>();

        // Add partitions on Polypheny level

        filesByPartitionValue.entrySet().stream()
                .sorted( Map.Entry.comparingByKey() )
                .forEach( entry -> {
                    String partitionName = partitionName( partitionColumn, entry.getKey() );
                    AllocationPartitionGroup group = allocationCatalog.addPartitionGroup(
                            logical.table.id,
                            partitionName,
                            logical.table.namespaceId,
                            PartitionType.LIST,
                            1,
                            false );
                    AllocationPartition partition = allocationCatalog.addPartition(
                            logical.table.id,
                            logical.table.namespaceId,
                            group.id,
                            partitionName,
                            false,
                            PlacementType.AUTOMATIC,
                            DataPlacementRole.REFRESHABLE,
                            List.of( entry.getKey() ),
                            PartitionType.LIST );
                    AllocationTable partitionAllocation = allocationCatalog.addAllocation(
                            adapterId,
                            originalAllocation.table.placementId,
                            partition.id,
                            logical.table.id );
                    PhysicalTable partitionPhysical = adapterCatalog.createTable(
                            logical.table.getNamespaceName(),
                            logical.table.name,
                            logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                            logical.table,
                            logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                            logical.pkIds,
                            AllocationTableWrapper.of( partitionAllocation, originalAllocation.columns, originalAllocation.physicalSchema ) );
                    ParquetTableBinding partitionBinding = new ParquetTableBinding(
                            entry.getValue(),
                            binding.parentTableName(),
                            binding.sourcePathElements(),
                            binding.columnsByColumnId() );
                    registerParquetBinding( partitionPhysical.id, partitionBinding );
                    var physical = currentNamespace.createParquetTable( partitionPhysical.id, partitionPhysical, partitionBinding, this );
                    adapterCatalog.replacePhysical( physical );
                    physicals.add( physical );
                    partitionGroupIds.add( group.id );
                    partitionIds.add( partition.id );
                } );

        allocationCatalog.addPartitionProperty(
                logical.table.id,
                PartitionProperty.builder()
                        .entityId( logical.table.id )
                        .partitionType( PartitionType.LIST )
                        .isPartitioned( true )
                        .partitionColumnId( partitionLogicalColumn.id )
                        .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                        .partitionIds( ImmutableList.copyOf( partitionIds ) )
                        .numPartitionGroups( partitionGroupIds.size() )
                        .numPartitions( partitionIds.size() )
                        .reliesOnPeriodicChecks( false )
                        .build() );
        catalog.updateSnapshot();
        return physicals;
    }


    private String partitionName( String columnName, String value ) {
        String normalized = (columnName + "_" + value).toLowerCase().replaceAll( "[^a-z0-9_]+", "_" );
        return normalized.isBlank() ? "partition" : normalized;
    }


    private void clearNormalizedExportCache() {
        normalizedSchema = null;
    }


    /**
     * get normalized schema from ParquetSchemaNormalizer
     *
     * @return ParquetNormalizedSchema
     */
    private ParquetNormalizedSchema getNormalizedSchema() {
        if ( connectionMethod == ConnectionMethod.UPLOAD && normalizedSchema != null ) {
            return normalizedSchema;
        }

        normalizedSchema = new ParquetSchemaNormalizer( parquetDir, getUniqueName() ).normalize();
        return normalizedSchema;
    }

}
