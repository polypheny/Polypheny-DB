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

package org.polypheny.db.ddl;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.IndexMethodModel;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPartitionGroup;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.catalog.logistic.NameGenerator;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.monitoring.events.DdlEvent;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty.PartitionCostIndication;
import org.polypheny.db.partition.raw.RawTemperaturePartitionInformation;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.view.MaterializedViewManager;


@Slf4j
public class DdlManagerImpl extends DdlManager {

    public static final String UNPARTITIONED = "part0";
    private final Catalog catalog;


    public DdlManagerImpl( Catalog catalog ) {
        this.catalog = catalog;
    }


    private void checkIfDdlPossible( EntityType entityType ) {
        if ( entityType == EntityType.SOURCE ) {
            throw new GenericRuntimeException( "Can not use DDLs on Sources" );
        }
    }


    private void checkViewDependencies( LogicalTable catalogTable ) {
        List<LogicalView> entities = catalog.getSnapshot().rel().getConnectedViews( catalogTable.id );
        if ( entities.isEmpty() ) {
            return;
        }
        List<String> views = new ArrayList<>();
        for ( LogicalView view : entities ) {
            views.add( view.name );
        }
        throw new GenericRuntimeException( "Cannot alter table because of underlying views: %s ", views.stream().map( String::valueOf ).collect( Collectors.joining( (", ") ) ) );

    }


    private LogicalColumn addDefaultValue( long namespaceId, PolyValue defaultValue, LogicalColumn column ) {
        if ( defaultValue != null ) {
            return catalog.getLogicalRel( namespaceId ).setDefaultValue( column.id, column.type, defaultValue );
        }
        return column;
    }


    protected DataStore<?> getDataStoreInstance( long storeId ) {
        Optional<Adapter<?>> optionalAdapter = AdapterManager.getInstance().getAdapter( storeId );
        if ( optionalAdapter.isEmpty() ) {
            throw new GenericRuntimeException( "Unknown storeId id: %i", storeId );
        }
        // Make sure it is a data storeId instance
        if ( optionalAdapter.get() instanceof DataStore<?> ) {
            return (DataStore<?>) optionalAdapter.get();
        } else if ( optionalAdapter.get() instanceof DataSource ) {
            throw new GenericRuntimeException( "Can not use DDLs on Sources" );
        } else {
            throw new GenericRuntimeException( "Unknown kind of adapter: %s", optionalAdapter.get().getClass().getName() );
        }
    }


    @Override
    public long createNamespace( String initialName, DataModel type, boolean ifNotExists, boolean replace, Statement statement ) {
        String name = initialName.toLowerCase();
        // Check that name is not blocked
        if ( blockedNamespaceNames.contains( name ) ) {
            throw new GenericRuntimeException( String.format( "Namespace name %s is not allowed.", name ) );
        }

        // Check if there is already a namespace with this name
        Optional<LogicalNamespace> optionalNamespace = catalog.getSnapshot().getNamespace( name );
        if ( optionalNamespace.isPresent() ) {
            if ( ifNotExists ) {
                // It is ok that there is already a namespace with this name because "IF NOT EXISTS" was specified
                return optionalNamespace.get().id;
            } else if ( replace ) {
                throw new GenericRuntimeException( "Replacing namespace is not yet supported." );
            }
        }
        boolean caseSensitive = type == DataModel.RELATIONAL
                ? RuntimeConfig.RELATIONAL_NAMESPACE_DEFAULT_CASE_SENSITIVE.getBoolean() : type == DataModel.DOCUMENT
                ? RuntimeConfig.DOCUMENT_NAMESPACE_DEFAULT_CASE_SENSITIVE.getBoolean() :
                type == DataModel.GRAPH && RuntimeConfig.GRAPH_NAMESPACE_DEFAULT_CASE_SENSITIVE.getBoolean();

        if ( type == DataModel.GRAPH ) {
            return createGraph( name, true, null, false, false, caseSensitive, statement );
        }

        return catalog.createNamespace( name, type, caseSensitive );
    }


    @Override
    public void createStore( String uniqueName, String adapterName, AdapterType adapterType, Map<String, String> config, DeployMode mode ) {
        uniqueName = uniqueName.toLowerCase();
        Adapter<?> adapter = AdapterManager.getInstance().addAdapter( adapterName, uniqueName, adapterType, mode, config );
    }


    @Override
    public void createSource( String uniqueName, String adapterName, long namespace, AdapterType adapterType, Map<String, String> config, DeployMode mode ) {
        uniqueName = uniqueName.toLowerCase();
        DataSource<?> adapter = (DataSource<?>) AdapterManager.getInstance().addAdapter( adapterName, uniqueName, adapterType, mode, config );

        Map<String, List<ExportedColumn>> exportedColumns;
        try {
            exportedColumns = adapter.getExportedColumns();
        } catch ( Exception e ) {
            AdapterManager.getInstance().removeAdapter( adapter.getAdapterId() );
            throw new GenericRuntimeException( "Could not deploy adapter", e );
        }
        // Create table, columns etc.
        for ( Map.Entry<String, List<ExportedColumn>> entry : exportedColumns.entrySet() ) {
            // Make sure the table name is unique
            String tableName = entry.getKey();
            if ( catalog.getSnapshot().rel().getTable( namespace, tableName ).isPresent() ) {
                int i = 0;
                while ( catalog.getSnapshot().rel().getTable( namespace, tableName + i ).isPresent() ) {
                    i++;
                }
                tableName += i;
            }

            LogicalTable logical = catalog.getLogicalRel( namespace ).addTable( tableName, EntityType.SOURCE, !(adapter).isDataReadOnly() );
            List<LogicalColumn> columns = new ArrayList<>();

            Pair<AllocationPartition, PartitionProperty> partitionProperty = createSinglePartition( logical.namespaceId, logical );

            AllocationPlacement placement = catalog.getAllocRel( namespace ).addPlacement( logical.id, namespace, adapter.adapterId );
            AllocationEntity allocation = catalog.getAllocRel( namespace ).addAllocation( adapter.getAdapterId(), placement.id, partitionProperty.left.id, logical.id );
            List<AllocationColumn> aColumns = new ArrayList<>();
            int colPos = 1;

            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                LogicalColumn column = catalog.getLogicalRel( namespace ).addColumn(
                        exportedColumn.name,
                        logical.id,
                        colPos++,
                        exportedColumn.type,
                        exportedColumn.collectionsType,
                        exportedColumn.length,
                        exportedColumn.scale,
                        exportedColumn.dimension,
                        exportedColumn.cardinality,
                        exportedColumn.nullable,
                        Collation.getDefaultCollation() );

                AllocationColumn allocationColumn = catalog.getAllocRel( namespace ).addColumn(
                        placement.id,
                        logical.id,
                        column.id,
                        adapter.adapterId,
                        PlacementType.STATIC,
                        exportedColumn.physicalPosition ); // Not a valid partitionGroupID --> placeholder

                columns.add( column );
                aColumns.add( allocationColumn );
            }

            buildNamespace( Catalog.defaultNamespaceId, logical, adapter );
            adapter.createTable( null, LogicalTableWrapper.of( logical, columns, List.of() ), AllocationTableWrapper.of( allocation.unwrap( AllocationTable.class ).orElseThrow(), aColumns ) );
            catalog.updateSnapshot();

        }
        catalog.updateSnapshot();

    }


    @Override
    public void dropAdapter( String name, Statement statement ) {
        name = name.replace( "'", "" );

        LogicalAdapter adapter = catalog.getSnapshot().getAdapter( name ).orElseThrow();
        if ( adapter.type == AdapterType.SOURCE ) {
            for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getEntitiesOnAdapter( adapter.id ).orElse( List.of() ) ) {
                // Make sure that there is only one adapter
                if ( catalog.getSnapshot().alloc().getFromLogical( allocation.logicalId ).size() != 1 ) {
                    throw new GenericRuntimeException( "The data source contains entities with more than one placement. This should not happen!" );
                }

                if ( allocation.unwrap( AllocationCollection.class ).isPresent() ) {
                    dropCollection( catalog.getSnapshot().doc().getCollection( allocation.adapterId ).orElseThrow(), statement );
                } else if ( allocation.unwrap( AllocationTable.class ).isPresent() ) {

                    for ( LogicalForeignKey fk : catalog.getSnapshot().rel().getForeignKeys( allocation.logicalId ) ) {
                        catalog.getLogicalRel( allocation.namespaceId ).deleteForeignKey( fk.id );
                    }

                    LogicalTable table = catalog.getSnapshot().rel().getTable( allocation.logicalId ).orElseThrow();

                    // Make sure that there is only one adapter
                    if ( catalog.getSnapshot().alloc().getPlacementsFromLogical( allocation.logicalId ).size() != 1 ) {
                        throw new GenericRuntimeException( "The data source contains tables with more than one placement. This should not happen!" );
                    }

                    // Make sure table is of type source
                    if ( table.entityType != EntityType.SOURCE ) {
                        throw new GenericRuntimeException( "Trying to drop a table located on a data source which is not of table type SOURCE. This should not happen!" );
                    }
                    // Delete column placement in catalog
                    for ( AllocationColumn column : allocation.unwrap( AllocationTable.class ).get().getColumns() ) {
                        catalog.getAllocRel( allocation.namespaceId ).deleteColumn( allocation.id, column.columnId );
                    }

                    // delete allocation
                    catalog.getAllocRel( allocation.namespaceId ).deleteAllocation( allocation.id );

                    // Remove primary keys
                    catalog.getLogicalRel( allocation.namespaceId ).deletePrimaryKey( table.id );

                    // Delete columns
                    for ( LogicalColumn column : catalog.getSnapshot().rel().getColumns( allocation.logicalId ) ) {
                        catalog.getLogicalRel( allocation.namespaceId ).deleteColumn( column.id );
                    }

                    // Delete the table
                    catalog.getLogicalRel( allocation.namespaceId ).deleteTable( table.id );
                    // Reset plan cache implementation cache & routing cache
                    statement.getQueryProcessor().resetCaches();
                }


            }
        }
        AdapterManager.getInstance().removeAdapter( adapter.id );
    }


    @Override
    public void renameNamespace( String newName, String currentName ) {
        newName = newName.toLowerCase();
        Optional<LogicalNamespace> optionalNamespace = catalog.getSnapshot().getNamespace( newName );
        if ( optionalNamespace.isPresent() ) {
            throw new GenericRuntimeException( "There is already a namespace with this name!" );
        }
        LogicalNamespace logicalNamespace = catalog.getSnapshot().getNamespace( currentName ).orElseThrow();
        catalog.renameNamespace( logicalNamespace.id, newName );
    }


    @Override
    public void addColumnToSourceTable( LogicalTable table, String columnPhysicalName, String columnLogicalName, String beforeColumnName, String afterColumnName, PolyValue defaultValue, Statement statement ) {

        if ( catalog.getSnapshot().rel().getColumn( table.id, columnLogicalName ).isEmpty() ) {
            throw new GenericRuntimeException( "There exist already a column with name %s on table %s", columnLogicalName, table.name );
        }

        LogicalColumn beforeColumn;
        beforeColumn = beforeColumnName == null ? null : catalog.getSnapshot().rel().getColumn( table.id, beforeColumnName ).orElseThrow();
        LogicalColumn afterColumn;
        afterColumn = afterColumnName == null ? null : catalog.getSnapshot().rel().getColumn( table.id, afterColumnName ).orElseThrow();

        // Make sure that the table is of table type SOURCE
        if ( table.entityType != EntityType.SOURCE ) {
            throw new GenericRuntimeException( "Illegal operation on table of type %s", table.entityType );
        }
        List<AllocationEntity> allocs = catalog.getSnapshot().alloc().getFromLogical( table.id );

        // Make sure there is only one adapter
        if ( allocs.size() != 1 ) {
            throw new GenericRuntimeException( "The table has an unexpected number of placements!" );
        }

        AllocationEntity allocation = allocs.get( 0 );

        long adapterId = allocation.adapterId;
        DataSource<?> dataSource = AdapterManager.getInstance().getSource( adapterId ).orElseThrow();

        //String physicalTableName = catalog.getSnapshot().alloc().getPhysicalTable( catalogTable.id, adapterId ).name;
        List<ExportedColumn> exportedColumns = dataSource.getExportedColumns().get( table.name );

        // Check if physicalColumnName is valid
        ExportedColumn exportedColumn = null;
        for ( ExportedColumn ec : exportedColumns ) {
            if ( ec.physicalColumnName.equalsIgnoreCase( columnPhysicalName ) ) {
                exportedColumn = ec;
            }
        }
        if ( exportedColumn == null ) {
            throw new GenericRuntimeException( "Invalid physical column name '%s'", columnPhysicalName );
        }

        int position = updateAdjacentPositions( table, beforeColumn, afterColumn );

        LogicalColumn addedColumn = catalog.getLogicalRel( table.namespaceId ).addColumn(
                columnLogicalName,
                table.id,
                position,
                exportedColumn.type,
                exportedColumn.collectionsType,
                exportedColumn.length,
                exportedColumn.scale,
                exportedColumn.dimension,
                exportedColumn.cardinality,
                exportedColumn.nullable,
                Collation.getDefaultCollation()
        );

        // Add default value
        addDefaultValue( table.namespaceId, defaultValue, addedColumn );

        // Add column placement
        catalog.getAllocRel( table.namespaceId ).addColumn(
                allocation.partitionId,
                table.id,
                addedColumn.id,
                dataSource.adapterId,
                PlacementType.STATIC,
                catalog.getSnapshot().alloc().getColumns( allocation.id ).size() );//Not a valid partitionID --> placeholder

        // Set column position
        // catalog.getAllocRel( catalogTable.namespaceId ).updateColumnPlacementPhysicalPosition( adapterId, addedColumn.id, exportedColumn.physicalPosition );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private int updateAdjacentPositions( LogicalTable catalogTable, LogicalColumn beforeColumn, LogicalColumn afterColumn ) {
        List<LogicalColumn> columns = sortByPosition( catalog.getSnapshot().rel().getColumns( catalogTable.id ) );
        int position = columns.size() + 1;
        if ( beforeColumn != null || afterColumn != null ) {
            if ( beforeColumn != null ) {
                position = beforeColumn.position;
            } else {
                position = afterColumn.position + 1;
            }
            // Update position of the other columns
            for ( int i = columns.size(); i >= position; i-- ) {
                updateColumnPosition( catalogTable, columns.get( i - 1 ), i + 1 );
            }
        }
        return position;
    }


    private void updateColumnPosition( LogicalTable table, LogicalColumn column, int position ) {
        catalog.getLogicalRel( table.namespaceId ).setColumnPosition( column.id, position );
    }


    @Override
    public void createColumn( String columnName, LogicalTable table, String beforeColumnName, String afterColumnName, ColumnTypeInformation type, boolean nullable, PolyValue defaultValue, Statement statement ) {
        columnName = adjustNameIfNeeded( columnName, table.namespaceId );
        // Check if the column either allows null values or has a default value defined.
        if ( defaultValue == null && !nullable ) {
            throw new GenericRuntimeException( "Column is not nullable and does not have a default value defined." );
        }

        if ( catalog.getSnapshot().rel().getColumn( table.id, columnName ).isPresent() ) {
            throw new GenericRuntimeException( "There already exists a column with name %s on table %s", columnName, table.name );
        }
        //
        LogicalColumn beforeColumn;
        beforeColumn = beforeColumnName == null ? null : catalog.getSnapshot().rel().getColumn( table.id, beforeColumnName ).orElseThrow();
        LogicalColumn afterColumn;
        afterColumn = afterColumnName == null ? null : catalog.getSnapshot().rel().getColumn( table.id, afterColumnName ).orElseThrow();

        int position = updateAdjacentPositions( table, beforeColumn, afterColumn );

        LogicalColumn addedColumn = catalog.getLogicalRel( table.namespaceId ).addColumn(
                columnName,
                table.id,
                position,
                type.type,
                type.collectionType,
                type.precision,
                type.scale,
                type.dimension,
                type.cardinality,
                nullable,
                Collation.getDefaultCollation()
        );

        // Add default value
        addedColumn = addDefaultValue( table.namespaceId, defaultValue, addedColumn );

        // Ask router on which stores this column shall be placed
        List<DataStore<?>> stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewRelField( addedColumn );

        // Add column on underlying data stores and insert default value
        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( store.getAdapterId(), table.id ).orElseThrow();

            catalog.getAllocRel( table.namespaceId ).addColumn(
                    placement.id,
                    // Will be set later
                    table.id,     // Will be set later
                    addedColumn.id,
                    store.adapterId,
                    PlacementType.AUTOMATIC,
                    catalog.getSnapshot().alloc().getColumns( placement.id ).size() );// we just append it at the end //Not a valid partitionID --> placeholder
            for ( AllocationEntity entity : catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ) ) {
                AdapterManager
                        .getInstance()
                        .getStore( store.getAdapterId() )
                        .orElseThrow()
                        .addColumn( statement.getPrepareContext(), entity.id, addedColumn );
            }
        }

        catalog.updateSnapshot();
        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void createForeignKey( LogicalTable table, LogicalTable refTable, List<String> columnNames, List<String> refColumnNames, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );
        checkIfDdlPossible( refTable.entityType );

        List<Long> columnIds = new ArrayList<>();
        for ( String columnName : columnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();
            columnIds.add( logicalColumn.id );
        }
        List<Long> referencesIds = new ArrayList<>();
        for ( String columnName : refColumnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( refTable.id, columnName ).orElseThrow();
            referencesIds.add( logicalColumn.id );
        }
        catalog.getLogicalRel( table.namespaceId ).addForeignKey( table.id, columnIds, refTable.id, referencesIds, constraintName, onUpdate, onDelete );
    }


    @Override
    public void createIndex( LogicalTable table, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, DataStore<?> location, Statement statement ) throws TransactionException {
        List<Long> columnIds = new ArrayList<>();
        for ( String columnName : columnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();
            columnIds.add( logicalColumn.id );
        }

        IndexType type = IndexType.MANUAL;

        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( table.entityType != EntityType.ENTITY && table.entityType != EntityType.MATERIALIZED_VIEW ) {
            throw new GenericRuntimeException( "It is only possible to add an index to a %s", table.entityType.name() );
        }

        // Check if there is already an index with this name for this table
        if ( catalog.getSnapshot().rel().getIndex( table.id, indexName ).isPresent() ) {
            throw new GenericRuntimeException( "There exist already an index with the name %s", indexName );
        }

        if ( location == null ) {
            if ( RuntimeConfig.DEFAULT_INDEX_PLACEMENT_STRATEGY.getEnum() == DefaultIndexPlacementStrategy.POLYPHENY ) { // Polystore Index
                createPolyphenyIndex( table, indexMethodName, columnNames, indexName, isUnique, statement );
            } else if ( RuntimeConfig.DEFAULT_INDEX_PLACEMENT_STRATEGY.getEnum() == DefaultIndexPlacementStrategy.ONE_DATA_STORE ) {
                if ( indexMethodName != null ) {
                    throw new GenericRuntimeException( "It is not possible to specify a index method if no location has been specified." );
                }
                // Find a storeId that has all required columns
                for ( AllocationPlacement placement : catalog.getSnapshot().alloc().getPlacementsFromLogical( table.id ) ) {
                    boolean hasAllColumns = true;
                    if ( !AdapterManager.getInstance().getStore( placement.adapterId ).orElseThrow().getAvailableIndexMethods().isEmpty() ) {
                        for ( long columnId : columnIds ) {
                            if ( catalog.getSnapshot().alloc().getAlloc( placement.id, columnId ).isEmpty() ) {
                                hasAllColumns = false;
                            }
                        }
                        if ( hasAllColumns ) {
                            location = AdapterManager.getInstance().getStore( placement.adapterId ).orElse( null );
                            break;
                        }
                    }
                }
                if ( location == null ) {
                    throw new GenericRuntimeException( "Unable to create an index on one of the underlying data stores since there is no data storeId that supports indexes and has all required columns!" );
                }
                addDataStoreIndex( table, indexMethodName, indexName, isUnique, location, statement, columnIds, type );
            } else if ( RuntimeConfig.DEFAULT_INDEX_PLACEMENT_STRATEGY.getEnum() == DefaultIndexPlacementStrategy.ALL_DATA_STORES ) {
                if ( indexMethodName != null ) {
                    throw new GenericRuntimeException( "It is not possible to specify a index method if no location has been specified." );
                }
                boolean createdAtLeastOne = false;
                for ( AllocationPlacement placement : catalog.getSnapshot().alloc().getPlacementsFromLogical( table.id ) ) {
                    boolean hasAllColumns = true;
                    if ( !AdapterManager.getInstance().getStore( placement.adapterId ).orElseThrow().getAvailableIndexMethods().isEmpty() ) {
                        for ( long columnId : columnIds ) {
                            if ( catalog.getSnapshot().alloc().getColumn( placement.id, columnId ).isEmpty() ) {
                                hasAllColumns = false;
                            }
                        }
                        if ( hasAllColumns ) {
                            DataStore<?> loc = AdapterManager.getInstance().getStore( placement.adapterId ).orElseThrow();
                            String name = indexName + "_" + loc.getUniqueName();
                            String nameSuffix = "";
                            int counter = 0;
                            while ( catalog.getSnapshot().rel().getIndex( table.id, name + nameSuffix ).isPresent() ) {
                                nameSuffix = String.valueOf( counter++ );
                            }
                            addDataStoreIndex( table, indexMethodName, name + nameSuffix, isUnique, loc, statement, columnIds, type );
                            createdAtLeastOne = true;
                        }
                    }
                }
                if ( !createdAtLeastOne ) {
                    throw new GenericRuntimeException( "Unable to create an index on one of the underlying data stores since there is no data store that supports indexes and has all required columns!" );
                }
            }
        } else { // Store Index
            addDataStoreIndex( table, indexMethodName, indexName, isUnique, location, statement, columnIds, type );
        }
    }


    private void addDataStoreIndex( LogicalTable table, String indexMethodName, String indexName, boolean isUnique, @NotNull DataStore<?> location, Statement statement, List<Long> columnIds, IndexType type ) {

        List<AllocationPartition> partitions = catalog.getSnapshot().alloc().getPartitionsFromLogical( table.id );
        if ( partitions.size() != 1 ) {
            throw new GenericRuntimeException( "It is not possible to create an index on a table with more than one partition." );
        }
        AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( location.getAdapterId(), table.id ).orElseThrow();

        AllocationTable alloc = catalog.getSnapshot().alloc().getAlloc( placement.id, partitions.get( 0 ).id ).orElseThrow().unwrap( AllocationTable.class ).orElseThrow();

        if ( !new HashSet<>( alloc.getColumns().stream().map( c -> c.columnId ).toList() ).containsAll( columnIds ) ) {
            throw new GenericRuntimeException( "Not all required columns for this index are placed on this storeId." );
        }

        String method;
        String methodDisplayName;
        if ( indexMethodName != null && !indexMethodName.equalsIgnoreCase( "default" ) ) {
            IndexMethodModel aim = null;
            for ( IndexMethodModel indexMethodModel : location.getAvailableIndexMethods() ) {
                if ( indexMethodModel.name().equals( indexMethodName ) ) {
                    aim = indexMethodModel;
                }
            }
            if ( aim == null ) {
                throw new GenericRuntimeException( "The used Index method is not known." );
            }
            method = aim.name();
            methodDisplayName = aim.displayName();
        } else {
            method = location.getDefaultIndexMethod().name();
            methodDisplayName = location.getDefaultIndexMethod().displayName();
        }

        LogicalIndex index = catalog.getLogicalRel( table.namespaceId ).addIndex(
                table.id,
                columnIds,
                isUnique,
                method,
                methodDisplayName,
                location.getAdapterId(),
                type,
                indexName );

        String physicalName = location.addIndex(
                statement.getPrepareContext(),
                index,
                alloc );
        catalog.getLogicalRel( table.namespaceId ).setIndexPhysicalName( index.id, physicalName );

    }


    public void createPolyphenyIndex( LogicalTable table, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, Statement statement ) throws TransactionException {
        indexName = indexName.toLowerCase();
        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();
            columnIds.add( logicalColumn.id );
        }

        IndexType type = IndexType.MANUAL;

        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( table.entityType != EntityType.ENTITY && table.entityType != EntityType.MATERIALIZED_VIEW ) {
            throw new GenericRuntimeException( "It is only possible to add an index to a %s", table.entityType.name() );
        }

        // Check if there is already an index with this name for this table
        if ( catalog.getSnapshot().rel().getIndex( table.id, indexName ).isPresent() ) {
            throw new GenericRuntimeException( "The already exists an index with this name %s", indexName );
        }

        String method;
        String methodDisplayName;
        if ( indexMethodName != null ) {
            IndexMethodModel aim = null;
            for ( IndexMethodModel indexMethodModel : IndexManager.getAvailableIndexMethods() ) {
                if ( indexMethodModel.name().equals( indexMethodName ) ) {
                    aim = indexMethodModel;
                }
            }
            if ( aim == null ) {
                throw new GenericRuntimeException( "The index method is not known" );
            }
            method = aim.name();
            methodDisplayName = aim.displayName();
        } else {
            method = IndexManager.getDefaultIndexMethod().name();
            methodDisplayName = IndexManager.getDefaultIndexMethod().displayName();
        }

        LogicalIndex index = catalog.getLogicalRel( table.namespaceId ).addIndex(
                table.id,
                columnIds,
                isUnique,
                method,
                methodDisplayName,
                -1,
                type,
                indexName );

        IndexManager.getInstance().addIndex( index, statement );
    }


    @Override
    public void createAllocationPlacement( LogicalTable table, List<LogicalColumn> newColumns, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore<?> dataStore, Statement statement ) {

        // check if allocation already exists
        if ( catalog.getSnapshot().alloc().getPlacement( dataStore.getAdapterId(), table.id ).isPresent() ) {
            throw new GenericRuntimeException( "The placement does already exist" );
        }

        List<LogicalColumn> adjustedColumns = new ArrayList<>( newColumns );

        // Check if placement includes primary key columns
        LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();

        for ( long cId : primaryKey.fieldIds ) {
            if ( newColumns.stream().noneMatch( c -> c.id == cId ) ) {
                adjustedColumns.add( catalog.getSnapshot().rel().getColumn( cId ).orElseThrow() );
            }
        }
        adjustedColumns = adjustedColumns.stream().sorted( Comparator.comparingLong( c -> c.position ) ).toList();

        AllocationPlacement placement = catalog.getAllocRel( table.namespaceId ).addPlacement( table.id, table.namespaceId, dataStore.adapterId );
        PartitionProperty property = catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();

        addAllocationsForPlacement( table.namespaceId, statement, table, placement.id, adjustedColumns, primaryKey.fieldIds, property.partitionIds, dataStore );

        Catalog.getInstance().updateSnapshot();

        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), catalog.getSnapshot().getAdapter( dataStore.getAdapterId() ).orElseThrow(), table, adjustedColumns, placement );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

    }


    @Override
    public void createPrimaryKey( LogicalTable table, List<String> columnNames, Statement statement ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        checkModelLogic( table );

        LogicalPrimaryKey oldPk = catalog.getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElse( null );

        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();
            columnIds.add( logicalColumn.id );
        }
        catalog.getLogicalRel( table.namespaceId ).addPrimaryKey( table.id, columnIds );

        // Add new column placements
        // long pkColumnId = oldPk.columnIds.get( 0 ); // It is sufficient to check for one because all get replicated on all stores
        List<AllocationPlacement> placements = catalog.getSnapshot().alloc().getPlacementsFromLogical( table.id );
        for ( AllocationPlacement placement : placements ) {
            List<Long> pColumnIds = catalog.getSnapshot().alloc().getColumns( placement.id ).stream().map( c -> c.columnId ).toList();
            for ( long columnId : columnIds ) {
                if ( !pColumnIds.contains( columnId ) ) {
                    catalog.getAllocRel( table.namespaceId ).addColumn(
                            placement.id,
                            // Will be set later
                            table.id,
                            columnId,
                            placement.adapterId,
                            PlacementType.AUTOMATIC,
                            0 );
                    for ( AllocationPartition partition : catalog.getSnapshot().alloc().getPartitionsFromLogical( table.id ) ) {
                        AllocationEntity entity = catalog.getSnapshot().alloc().getAlloc( placement.id, partition.id ).orElseThrow();
                        AdapterManager.getInstance().getStore( placement.adapterId ).orElseThrow().addColumn(
                                statement.getPrepareContext(),
                                entity.id,
                                catalog.getSnapshot().rel().getColumn( columnId ).orElseThrow() );
                    }
                }
            }
        }

    }


    @Override
    public void createUniqueConstraint( LogicalTable table, List<String> columnNames, String constraintName ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        checkModelLogic( table, null );

        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();
            columnIds.add( logicalColumn.id );
        }
        catalog.getLogicalRel( table.namespaceId ).addUniqueConstraint( table.id, constraintName, columnIds );

    }


    @Override
    public void dropColumn( LogicalTable table, String columnName, Statement statement ) {
        List<LogicalColumn> columns = catalog.getSnapshot().rel().getColumns( table.id );
        if ( columns.size() < 2 ) {
            throw new GenericRuntimeException( "Cannot drop sole column of table %s", table.name );
        }

        // check if model permits operation
        checkModelLogic( table, columnName );

        //check if views are dependent from this table
        checkViewDependencies( table );

        LogicalColumn column = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        LogicalRelSnapshot snapshot = catalog.getSnapshot().rel();

        // Check if column is part of a key
        for ( LogicalKey key : snapshot.getTableKeys( table.id ) ) {
            if ( key.fieldIds.contains( column.id ) ) {
                if ( snapshot.isPrimaryKey( key.id ) ) {
                    throw new GenericRuntimeException( "Cannot drop column '" + column.name + "' because it is part of the primary key." );
                } else if ( snapshot.isIndex( key.id ) ) {
                    throw new GenericRuntimeException( "Cannot drop column '" + column.name + "' because it is part of the index with the name: '" + snapshot.getIndexes( key ).get( 0 ).name + "'." );
                } else if ( snapshot.isForeignKey( key.id ) ) {
                    throw new GenericRuntimeException( "Cannot drop column '" + column.name + "' because it is part of the foreign key with the name: '" + snapshot.getForeignKeys( key ).get( 0 ).name + "'." );
                } else if ( snapshot.isConstraint( key.id ) ) {
                    throw new GenericRuntimeException( "Cannot drop column '" + column.name + "' because it is part of the constraint with the name: '" + snapshot.getConstraints( key ).get( 0 ).name + "'." );
                }
                throw new GenericRuntimeException( "Ok, strange... Something is going wrong here!" );
            }
        }

        for ( AllocationColumn allocationColumn : catalog.getSnapshot().alloc().getColumnFromLogical( column.id ).orElseThrow() ) {
            deleteAllocationColumn( table, statement, allocationColumn );
        }

        // Delete from catalog
        catalog.getLogicalRel( table.namespaceId ).deleteColumn( column.id );
        if ( column.position != columns.size() ) {
            // Update position of the other columns
            for ( int i = column.position; i < columns.size(); i++ ) {
                catalog.getLogicalRel( table.namespaceId ).setColumnPosition( columns.get( i ).id, i );
            }
        }

        // Monitor dropColumn for statistics
        prepareMonitoring( statement, Kind.DROP_COLUMN, table, column );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private void deleteAllocationColumn( LogicalTable table, Statement statement, AllocationColumn allocationColumn ) {
        if ( table.entityType == EntityType.ENTITY ) {
            for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getAllocsOfPlacement( allocationColumn.placementId ) ) {
                AdapterManager.getInstance().getStore( allocationColumn.adapterId )
                        .orElseThrow()
                        .dropColumn(
                                statement.getPrepareContext(),
                                allocation.id,
                                allocationColumn.columnId );
            }

        }
        catalog.getAllocRel( table.namespaceId ).deleteColumn( allocationColumn.placementId, allocationColumn.columnId );
    }


    private void checkModelLogic( LogicalTable catalogTable ) {
        if ( catalogTable.dataModel == DataModel.DOCUMENT ) {
            throw new GenericRuntimeException( "Modification operation is not allowed by schema type DOCUMENT" );
        }
    }


    private void checkModelLogic( LogicalTable catalogTable, String columnName ) {
        if ( catalogTable.dataModel == DataModel.DOCUMENT
                && (columnName.equals( DocumentType.DOCUMENT_DATA ) || columnName.equals( DocumentType.DOCUMENT_ID )) ) {
            throw new GenericRuntimeException( "Modification operation is not allowed by schema type DOCUMENT" );
        }
    }


    @Override
    public void dropConstraint( LogicalTable table, String constraintName ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        LogicalConstraint constraint = catalog.getSnapshot().rel().getConstraint( table.id, constraintName ).orElseThrow();

        Supplier<Boolean> stillUsed = () -> getKeyUniqueCount( constraint.keyId ) < 2;
        if ( constraint.type == ConstraintType.UNIQUE && isForeignKey( constraint.key.id ) && stillUsed.get() ) {
            // maybe we delete multiple constraints in this transaction, so we need to check again
            catalog.attachCommitConstraint(
                    stillUsed,
                    "The constraint " + constraintName + " is used on a key which is referenced by at least one foreign key which requires this key to be unique. Unable to drop unique constraint." );
        }

        catalog.getLogicalRel( table.namespaceId ).deleteConstraint( constraint.id );
    }


    private boolean isForeignKey( long key ) {
        return Catalog.snapshot().rel().getKeys().stream().filter( k -> k instanceof LogicalForeignKey ).map( k -> (LogicalForeignKey) k ).anyMatch( k -> k.referencedKeyId == key );
    }


    private int getKeyUniqueCount( long keyId ) {
        int count = 0;
        if ( Catalog.snapshot().rel().getPrimaryKey( keyId ).isPresent() ) {
            count++;
        }

        for ( LogicalConstraint constraint : Catalog.snapshot().rel().getConstraints().stream().filter( c -> c.keyId == keyId ).toList() ) {
            if ( constraint.type == ConstraintType.UNIQUE ) {
                count++;
            }
        }

        for ( LogicalIndex index : Catalog.snapshot().rel().getIndexes().stream().filter( i -> i.keyId == keyId ).toList() ) {
            if ( index.unique ) {
                count++;
            }
        }

        return count;
    }


    @Override
    public void dropForeignKey( LogicalTable table, String foreignKeyName ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        if ( !table.modifiable ) {
            throw new GenericRuntimeException( "Not possible to use ALTER TABLE because %s is not a table.", table.name );
        }

        LogicalForeignKey foreignKey = catalog.getSnapshot().rel().getForeignKey( table.id, foreignKeyName ).orElseThrow();
        catalog.getLogicalRel( table.namespaceId ).deleteForeignKey( foreignKey.id );
    }


    @Override
    public void dropIndex( LogicalTable table, String indexName, Statement statement ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        LogicalIndex index = catalog.getSnapshot().rel().getIndex( table.id, indexName ).orElseThrow();

        if ( index.location < 0 ) {
            IndexManager.getInstance().deleteIndex( index );
        } else {
            DataStore<?> store = AdapterManager.getInstance().getStore( index.location ).orElseThrow();
            AllocationPlacement placement = Catalog.snapshot().alloc().getPlacement( store.getAdapterId(), table.id ).orElseThrow();
            catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ).forEach( allocation -> {
                store.dropIndex( statement.getPrepareContext(), index, List.of( allocation.id ) );
            } );
        }

        catalog.getLogicalRel( table.namespaceId ).deleteIndex( index.id );
    }


    @Override
    public void dropPlacement( LogicalTable table, DataStore<?> store, Statement statement ) {
        // Check whether this placement exists
        AllocationPlacement placement = catalog
                .getSnapshot()
                .alloc().getPlacement( store.adapterId, table.id ).orElseThrow();

        if ( !validatePlacementsConstraints( placement, catalog.getSnapshot().alloc().getColumns( placement.id ), List.of() ) ) {
            throw new GenericRuntimeException( "The last placement cannot be deleted" );
        }

        // Drop all indexes on this storeId
        for ( LogicalIndex index : catalog.getSnapshot().rel().getIndexes( table.id, false ) ) {
            if ( index.location == store.getAdapterId() ) {
                if ( index.location < 0 ) {
                    // Delete polystore index
                    IndexManager.getInstance().deleteIndex( index );
                } else {
                    // Delete index on storeId
                    AdapterManager.getInstance().getStore( index.location )
                            .orElseThrow()
                            .dropIndex(
                                    statement.getPrepareContext(),
                                    index, catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( index.location, table.id ) );
                }
                // Delete index in catalog
                catalog.getLogicalRel( table.namespaceId ).deleteIndex( index.id );
            }
        }

        for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ) ) {
            dropAllocation( table.namespaceId, store, statement, allocation.id );
        }

        for ( AllocationColumn column : catalog.getSnapshot().alloc().getColumns( placement.id ) ) {
            catalog.getAllocRel( column.namespaceId ).deleteColumn( column.placementId, column.columnId );
        }

        // remove placement itself
        catalog.getAllocRel( table.namespaceId ).deletePlacement( placement.id );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private void dropAllocation( long namespaceId, DataStore<?> store, Statement statement, long allocId ) {
        // Physically delete the data from the storeId
        store.dropTable( statement.getPrepareContext(), allocId );

        catalog.getAllocRel( namespaceId ).deleteAllocation( allocId );
    }


    @Override
    public void dropPrimaryKey( LogicalTable table ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );
        catalog.getLogicalRel( table.namespaceId ).deletePrimaryKey( table.id );
    }


    @Override
    public void setColumnType( LogicalTable table, String columnName, ColumnTypeInformation type, Statement statement ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        // check if model permits operation
        checkModelLogic( table, columnName );

        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        catalog.getLogicalRel( table.namespaceId ).setColumnType(
                logicalColumn.id,
                type.type,
                type.collectionType,
                type.precision,
                type.scale,
                type.dimension,
                type.cardinality );
        catalog.updateSnapshot();
        for ( AllocationColumn allocationColumn : catalog.getSnapshot().alloc().getColumnFromLogical( logicalColumn.id ).orElseThrow() ) {
            for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getAllocsOfPlacement( allocationColumn.placementId ) ) {
                AdapterManager.getInstance().getStore( allocationColumn.adapterId )
                        .orElseThrow()
                        .updateColumnType(
                                statement.getPrepareContext(),
                                allocation.id,
                                catalog.getSnapshot().rel().getColumn( logicalColumn.id ).orElseThrow() );
            }
        }

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setColumnNullable( LogicalTable table, String columnName, boolean nullable, Statement statement ) {
        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        // Check if model permits operation
        checkModelLogic( table, columnName );

        catalog.getLogicalRel( table.namespaceId ).setNullable( logicalColumn.id, nullable );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setColumnPosition( LogicalTable table, String columnName, String beforeColumnName, String afterColumnName, Statement statement ) {
        // Check if model permits operation
        checkModelLogic( table, columnName );

        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        int targetPosition;
        LogicalColumn refColumn;
        if ( beforeColumnName != null ) {
            refColumn = catalog.getSnapshot().rel().getColumn( table.id, beforeColumnName ).orElseThrow();
            targetPosition = refColumn.position;
        } else {
            refColumn = catalog.getSnapshot().rel().getColumn( table.id, afterColumnName ).orElseThrow();
            targetPosition = refColumn.position + 1;
        }
        if ( logicalColumn.id == refColumn.id ) {
            throw new GenericRuntimeException( "Same column!" );
        }
        List<LogicalColumn> columns = catalog.getSnapshot().rel().getColumns( table.id );
        if ( targetPosition < logicalColumn.position ) {  // Walk from last column to first column
            for ( int i = columns.size(); i >= 1; i-- ) {
                if ( i < logicalColumn.position && i >= targetPosition ) {
                    catalog.getLogicalRel( table.namespaceId ).setColumnPosition( columns.get( i - 1 ).id, i + 1 );
                } else if ( i == logicalColumn.position ) {
                    catalog.getLogicalRel( table.namespaceId ).setColumnPosition( logicalColumn.id, columns.size() + 1 );
                }
                if ( i == targetPosition ) {
                    catalog.getLogicalRel( table.namespaceId ).setColumnPosition( logicalColumn.id, targetPosition );
                }
            }
        } else if ( targetPosition > logicalColumn.position ) { // Walk from first column to last column
            targetPosition--;
            for ( int i = 1; i <= columns.size(); i++ ) {
                if ( i > logicalColumn.position && i <= targetPosition ) {
                    catalog.getLogicalRel( table.namespaceId ).setColumnPosition( columns.get( i - 1 ).id, i - 1 );
                } else if ( i == logicalColumn.position ) {
                    catalog.getLogicalRel( table.namespaceId ).setColumnPosition( logicalColumn.id, columns.size() + 1 );
                }
                if ( i == targetPosition ) {
                    catalog.getLogicalRel( table.namespaceId ).setColumnPosition( logicalColumn.id, targetPosition );
                }
            }
        }
        // Do nothing

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setColumnCollation( LogicalTable table, String columnName, Collation collation, Statement statement ) {
        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        // Check if model permits operation
        checkModelLogic( table, columnName );

        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        catalog.getLogicalRel( table.namespaceId ).setCollation( logicalColumn.id, collation );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setDefaultValue( LogicalTable table, String columnName, PolyValue defaultValue, Statement statement ) {
        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        // Check if model permits operation
        checkModelLogic( table, columnName );

        addDefaultValue( table.namespaceId, defaultValue, logicalColumn );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropDefaultValue( LogicalTable table, String columnName, Statement statement ) {
        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        // check if model permits operation
        checkModelLogic( table, columnName );

        catalog.getLogicalRel( table.namespaceId ).deleteDefaultValue( logicalColumn.id );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void modifyPlacement( LogicalTable table, List<Long> columns, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore<?> store, Statement statement ) {
        Optional<AllocationPlacement> placementOptional = statement.getDataContext().getSnapshot().alloc().getPlacement( store.getAdapterId(), table.id );
        // Check whether this placement exists
        if ( placementOptional.isEmpty() ) {
            throw new GenericRuntimeException( "The requested placement does not exists" );
        }

        // get current columns on placement
        List<Long> currentColumns = catalog.getSnapshot()
                .alloc()
                .getColumns( placementOptional.get().id )
                .stream()
                .map( c -> c.columnId )
                .toList();

        // all
        List<Long> toRemove = currentColumns
                .stream()
                .filter( c -> !columns.contains( c ) )
                .toList();

        List<Long> toAdd = columns.stream().filter( c -> !currentColumns.contains( c ) ).toList();

        // Check if views are dependent from this
        checkViewDependencies( table );

        // add columns
        for ( long columnId : toAdd ) {
            createColumnPlacement( table, Catalog.snapshot().rel().getColumn( columnId ).orElseThrow(), store, statement );
        }
        catalog.updateSnapshot();

        // Checks before physically removing of placement that the partition distribution is still valid and sufficient
        dropPlacementColumns( table, toRemove, store, statement, placementOptional.get() );
        catalog.updateSnapshot();

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private void dropPlacementColumns( LogicalTable table, List<Long> columns, DataStore<?> store, Statement statement, AllocationPlacement placement ) {
        List<AllocationColumn> columnsToRemove = new ArrayList<>();

        LogicalRelSnapshot snapshot = statement.getTransaction().getSnapshot().rel();

        // Identifies which columns need to be removed
        for ( AllocationColumn allocationColumn : catalog.getSnapshot().alloc().getColumns( placement.id ) ) {
            if ( columns.contains( allocationColumn.columnId ) ) {
                checkIndexDependent( table, store, snapshot, allocationColumn );
                // Check whether the column is a primary key column
                LogicalPrimaryKey primaryKey = snapshot.getPrimaryKey( table.primaryKey ).orElseThrow();
                if ( primaryKey.fieldIds.contains( allocationColumn.columnId ) ) {
                    // Check if the placement type is manual. If so, change to automatic
                    if ( allocationColumn.placementType == PlacementType.MANUAL ) {
                        // Make placement manual
                        catalog.getAllocRel( table.namespaceId ).updateColumnPlacementType(
                                store.getAdapterId(),
                                allocationColumn.columnId,
                                PlacementType.AUTOMATIC );
                    }
                } else {
                    // It is not a primary key. Remove the column
                    columnsToRemove.add( allocationColumn );
                }
            }
        }

        if ( !validatePlacementsConstraints( placement, columnsToRemove, List.of() ) ) {
            throw new GenericRuntimeException( "Cannot remove placement as it is the last" );
        }

        // Remove columns physically
        columnsToRemove.forEach( column -> deleteAllocationColumn( table, statement, column ) );
    }


    private static void checkIndexDependent( LogicalTable table, DataStore<?> store, LogicalRelSnapshot snapshot, AllocationColumn allocationColumn ) {
        // Check whether there are any indexes located on the storeId requiring this column
        for ( LogicalIndex index : snapshot.getIndexes( table.id, false ) ) {
            if ( index.location == store.getAdapterId() && index.key.fieldIds.contains( allocationColumn.columnId ) ) {
                throw new GenericRuntimeException( "The index with name %s depends on the columns %s", index.name, snapshot.getColumn( allocationColumn.columnId ).map( c -> c.name ).orElse( "null" ) );
            }
        }
    }


    /**
     * Checks if the planned changes are allowed in terms of placements that need to be present.
     * Each column must be present for all partitions somewhere.
     *
     * @param columnsToBeRemoved columns that shall be removed
     * @param partitionsIdsToBeRemoved partitions that shall be removed
     * @return true if these changes can be made to the data placement, false if not
     */
    public boolean validatePlacementsConstraints( AllocationPlacement placement, List<AllocationColumn> columnsToBeRemoved, List<Long> partitionsIdsToBeRemoved ) {
        if ( (columnsToBeRemoved.isEmpty() && partitionsIdsToBeRemoved.isEmpty()) ) {
            log.warn( "Invoked validation with two empty lists of columns and partitions to be revoked. Is therefore always true..." );
            return true;
        }
        List<Long> columnIdsToRemove = columnsToBeRemoved.stream().map( c -> c.columnId ).toList();

        // TODO @HENNLO Focus on PartitionPlacements that are labeled as UPTODATE nodes. The outdated nodes do not
        //  necessarily need placement constraints

        LogicalTable table = catalog.getSnapshot().rel().getTable( placement.logicalEntityId ).orElseThrow();
        List<AllocationPlacement> dataPlacements = catalog.getSnapshot().alloc().getPlacementsFromLogical( table.id );

        List<LogicalColumn> columns = catalog.getSnapshot().rel().getColumns( table.id );

        // Checks for every column on every DataPlacement if each column is placed with all partitions
        for ( LogicalColumn column : columns ) {
            List<Long> partitionsToBeCheckedForColumn = new ArrayList<>( catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow().partitionIds );
            // Check for every column if it has every partition
            for ( AllocationPlacement dataPlacement : dataPlacements ) {
                // Can instantly return because we still have a full placement somewhere
                if ( catalog.getSnapshot().alloc().getColumns( dataPlacement.id ).size() == columns.size() && dataPlacement.adapterId != placement.adapterId ) {
                    return true;
                }

                List<Long> effectiveColumnsOnStore = new ArrayList<>( catalog.getSnapshot().alloc().getColumns( dataPlacement.id ).stream().map( c -> c.columnId ).toList() );
                List<Long> effectivePartitionsOnStore = new ArrayList<>( catalog.getSnapshot().alloc().getPartitionsFromLogical( dataPlacement.logicalEntityId ).stream().map( p -> p.id ).toList() );

                // Remove columns and partitions from storeId to not evaluate them
                if ( dataPlacement.adapterId == placement.adapterId ) {

                    // Skips columns that shall be removed
                    if ( columnIdsToRemove.contains( column.id ) ) {
                        continue;
                    }

                    // Only process those parts that shall be present after change
                    effectiveColumnsOnStore.removeAll( columnIdsToRemove );
                    effectivePartitionsOnStore.removeAll( partitionsIdsToBeRemoved );
                }

                if ( effectiveColumnsOnStore.contains( column.id ) ) {
                    partitionsToBeCheckedForColumn.removeAll( effectivePartitionsOnStore );
                } else {
                    continue;
                }

                // Found all partitions for column, continue with next column
                if ( partitionsToBeCheckedForColumn.isEmpty() ) {
                    break;
                }
            }

            if ( !partitionsToBeCheckedForColumn.isEmpty() ) {
                return false;
            }
        }

        return true;
    }


    @Override
    public void modifyPartitionPlacement( LogicalTable table, List<Long> partitionIds, DataStore<?> store, Statement statement ) {
        long storeId = store.getAdapterId();

        AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( storeId, table.id ).orElseThrow();
        List<AllocationEntity> currentAllocs = catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id );
        List<Long> currentPartitionsId = currentAllocs.stream().map( a -> a.partitionId ).toList();

        // Get PartitionGroups that have been removed
        List<AllocationEntity> removedPartitions = currentAllocs.stream().filter( a -> !partitionIds.contains( a.partitionId ) ).toList();
        List<Long> addedPartitions = partitionIds.stream().filter( id -> !currentPartitionsId.contains( id ) ).toList();

        // Copy the data to the newly added column placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        List<AllocationColumn> allocationColumns = catalog.getSnapshot().alloc().getColumns( placement.id );
        List<LogicalColumn> columns = allocationColumns.stream().map( c -> catalog.getSnapshot().rel().getColumn( c.columnId ).orElseThrow() ).toList();

        if ( !addedPartitions.isEmpty() ) {

            for ( long partitionId : addedPartitions ) {
                AllocationTable allocation = addAllocationTable( table.namespaceId, statement, table, columns, List.of(), placement.id, partitionId, allocationColumns, store );
                dataMigrator.copyData( statement.getTransaction(), catalog.getSnapshot().getAdapter( storeId ).orElseThrow(), table, columns, allocation );
            }

            // Add indexes on this new Partition Placement if there is already an index
            for ( LogicalIndex currentIndex : catalog.getSnapshot().rel().getIndexes( table.id, false ) ) {
                if ( currentIndex.location == storeId ) {
                    store.addIndex( statement.getPrepareContext(), currentIndex, catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ).stream().map( a -> a.unwrap( AllocationTable.class ).orElseThrow() ).toList() );
                }
            }
        }

        if ( !removedPartitions.isEmpty() ) {
            //  Remove indexes
            for ( LogicalIndex currentIndex : catalog.getSnapshot().rel().getIndexes( table.id, false ) ) {
                if ( currentIndex.location == storeId ) {
                    store.dropIndex( null, currentIndex, removedPartitions.stream().map( p -> p.partitionId ).toList() );
                }
            }
            for ( AllocationEntity removedPartition : removedPartitions ) {
                dropAllocation( table.namespaceId, store, statement, removedPartition.id );
            }

        }
        catalog.updateSnapshot();

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void createColumnPlacement( LogicalTable table, LogicalColumn logicalColumn, DataStore<?> store, Statement statement ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        // Check whether this placement already exists
        Optional<AllocationPlacement> optPlacement = snapshot.alloc().getPlacement( store.getAdapterId(), table.id );

        if ( optPlacement.isEmpty() ) {
            throw new GenericRuntimeException( "The requested placement does not exist" );
        }

        AllocationPlacement placement = optPlacement.orElseThrow();

        Optional<AllocationColumn> optionalColumn = catalog.getSnapshot().alloc().getColumn( placement.id, logicalColumn.id );
        // Make sure that this storeId does not contain a placement of this column
        if ( optionalColumn.isPresent() ) {
            if ( optionalColumn.get().placementType != PlacementType.AUTOMATIC ) {
                throw new GenericRuntimeException( "There already exist a placement" );
            }

            // Make placement manual
            catalog.getAllocRel( table.namespaceId ).updateColumnPlacementType(
                    store.getAdapterId(),
                    logicalColumn.id,
                    PlacementType.MANUAL );

        } else {
            // Create column placement
            catalog.getAllocRel( table.namespaceId ).addColumn(
                    placement.id,
                    table.id,
                    logicalColumn.id,
                    store.adapterId,
                    PlacementType.MANUAL,
                    logicalColumn.position );

            for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ) ) {
                // Add column on storeId
                store.addColumn( statement.getPrepareContext(), allocation.id, logicalColumn );
                // Copy the data to the newly added column placements
                DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
                dataMigrator.copyData(
                        statement.getTransaction(),
                        catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(),
                        table,
                        List.of( logicalColumn ),
                        allocation );
            }


        }

        catalog.updateSnapshot();

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropColumnPlacement( LogicalTable table, LogicalColumn column, DataStore<?> store, Statement statement ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        // Check whether this placement even exists
        Optional<AllocationPlacement> optionalPlacement = snapshot.alloc().getPlacement( store.getAdapterId(), table.id );
        if ( optionalPlacement.isEmpty() ) {
            throw new GenericRuntimeException( "The placement does not exist" );
        }

        // Check whether this storeId actually contains a placement of this column
        if ( catalog.getSnapshot().alloc().getColumn( optionalPlacement.get().id, column.id ).isEmpty() ) {
            throw new GenericRuntimeException( "The placement does not exist on the storeId" );
        }
        // Check whether there are any indexes located on the storeId requiring this column
        for ( LogicalIndex index : catalog.getSnapshot().rel().getIndexes( table.id, false ) ) {
            if ( index.location == store.getAdapterId() && index.key.fieldIds.contains( column.id ) ) {
                throw new GenericRuntimeException( "Cannot remove the column %s, as there is a index %s using it", column, index.name );
            }
        }

        if ( !validatePlacementsConstraints( optionalPlacement.get(), List.of( catalog.getSnapshot().alloc().getColumn( optionalPlacement.get().id, column.id ).orElseThrow() ), List.of() ) ) {
            throw new GenericRuntimeException( "Cannot drop the placement as it is the last" );
        }

        // Check whether the column to drop is a primary key
        LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
        if ( primaryKey.fieldIds.contains( column.id ) ) {
            throw new GenericRuntimeException( "Cannot drop primary key" );
        }
        for ( AllocationPartition partition : catalog.getSnapshot().alloc().getPartitionsFromLogical( table.id ) ) {
            AllocationEntity allocation = catalog.getSnapshot().alloc().getAlloc( optionalPlacement.get().id, partition.id ).orElseThrow();
            // Drop Column on store
            store.dropColumn( statement.getPrepareContext(), allocation.id, column.id );
        }
        // Drop column placement
        catalog.getAllocRel( table.namespaceId ).deleteColumn( optionalPlacement.get().id, column.id );

        catalog.updateSnapshot();

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void renameTable( LogicalTable table, String newTableName, Statement statement ) {
        if ( catalog.getSnapshot().rel().getTable( table.namespaceId, newTableName ).isPresent() ) {
            throw new GenericRuntimeException( "An entity with name %s already exists", newTableName );
        }
        // Check if views are dependent from this view
        checkViewDependencies( table );

        if ( catalog.getSnapshot().getNamespace( table.namespaceId ).orElseThrow().caseSensitive ) {
            newTableName = newTableName.toLowerCase();
        }

        catalog.getLogicalRel( table.namespaceId ).renameTable( table.id, newTableName );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void renameCollection( LogicalCollection collection, String newName, Statement statement ) {
        if ( catalog.getSnapshot().rel().getTable( collection.namespaceId, newName ).isPresent() ) {
            throw new GenericRuntimeException( "An entity with name %s already exists", newName );
        }
        // Check if views are dependent from this view
        //checkViewDependencies( collection );

        if ( !catalog.getSnapshot().getNamespace( collection.namespaceId ).orElseThrow().caseSensitive ) {
            newName = newName.toLowerCase();
        }

        catalog.getLogicalDoc( collection.namespaceId ).renameCollection( collection, newName );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void renameColumn( LogicalTable table, String columnName, String newColumnName, Statement statement ) {
        LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        if ( catalog.getSnapshot().rel().getColumn( table.id, newColumnName ).isPresent() ) {
            throw new GenericRuntimeException( "There already exists a column with name %s on table %s", newColumnName, logicalColumn.getTableName() );
        }
        // Check if views are dependent from this view
        checkViewDependencies( table );

        catalog.getLogicalRel( table.namespaceId ).renameColumn( logicalColumn.id, newColumnName );

        if ( table.entityType != EntityType.VIEW ) {
            List<AllocationPlacement> placements = catalog.getSnapshot().alloc().getPlacementsOfColumn( logicalColumn.id );
            placements.forEach( p -> AdapterManager.getInstance().getAdapter( p.adapterId ).orElseThrow().renameLogicalColumn( logicalColumn.id, newColumnName ) );
        }

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void createView( String viewName, long namespaceId, AlgNode algNode, AlgCollation algCollation, boolean replace, Statement statement, PlacementType placementType, List<String> projectedColumns, String query, QueryLanguage language ) {
        viewName = adjustNameIfNeeded( viewName, namespaceId );

        if ( catalog.getSnapshot().rel().getTable( namespaceId, viewName ).isPresent() ) {
            if ( replace ) {
                dropView( catalog.getSnapshot().rel().getTable( namespaceId, viewName ).orElseThrow(), statement );
            } else {
                throw new GenericRuntimeException( "There already exists a view with the name %s", viewName );
            }
        }

        AlgDataType fieldList = algNode.getTupleType();

        List<FieldInformation> columns = getColumnInformation( projectedColumns, fieldList );

        Map<Long, List<Long>> underlyingTables = new HashMap<>();

        findUnderlyingTablesOfView( algNode, underlyingTables, fieldList );

        // add check if underlying table is of model document -> mql, relational -> sql
        underlyingTables.keySet().forEach( tableId -> checkModelLangCompatibility( language.dataModel(), namespaceId ) );

        LogicalView view = catalog.getLogicalRel( namespaceId ).addView(
                viewName,
                namespaceId,
                false,
                algNode,
                algCollation,
                underlyingTables,
                List.of(),
                fieldList,
                query,
                language
        );

        for ( FieldInformation column : columns ) {
            catalog.getLogicalRel( namespaceId ).addColumn(
                    column.name,
                    view.id,
                    column.position,
                    column.typeInformation.type,
                    column.typeInformation.collectionType,
                    column.typeInformation.precision,
                    column.typeInformation.scale,
                    column.typeInformation.dimension,
                    column.typeInformation.cardinality,
                    column.typeInformation.nullable,
                    column.collation );
        }

        catalog.updateSnapshot();
    }


    private String adjustNameIfNeeded( String name, long namespaceId ) {
        if ( !catalog.getSnapshot().getNamespace( namespaceId ).orElseThrow().caseSensitive ) {
            return name.toLowerCase();
        }
        return name;
    }


    @Override
    public void createMaterializedView( String viewName, long namespaceId, AlgRoot algRoot, boolean replace, Statement statement, List<DataStore<?>> stores, PlacementType placementType, List<String> projectedColumns, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ifNotExists, boolean ordered ) {
        viewName = adjustNameIfNeeded( viewName, namespaceId );

        // Check if there is already a table with this name
        if ( assertEntityExists( namespaceId, viewName, ifNotExists ) ) {
            return;
        }

        if ( stores == null ) {
            // Ask router on which storeId(s) the table should be placed
            stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewEntity();
        }

        AlgDataType fieldList = algRoot.alg.getTupleType();

        Map<Long, List<Long>> underlyingTables = new HashMap<>();
        Map<Long, List<Long>> underlying = findUnderlyingTablesOfView( algRoot.alg, underlyingTables, fieldList );

        Snapshot snapshot = statement.getTransaction().getSnapshot();
        LogicalRelSnapshot relSnapshot = snapshot.rel();

        // add check if underlying table is of model document -> mql, relational -> sql
        underlying.keySet().forEach( tableId -> checkModelLangCompatibility( language.dataModel(), namespaceId ) );

        if ( materializedCriteria.getCriteriaType() == CriteriaType.UPDATE ) {
            List<EntityType> entityTypes = new ArrayList<>();
            underlying.keySet().forEach( t -> entityTypes.add( relSnapshot.getTable( t ).orElseThrow().entityType ) );
            if ( !(entityTypes.contains( EntityType.ENTITY )) ) {
                throw new GenericRuntimeException( "Not possible to use Materialized View with Update Freshness if underlying table does not include a modifiable table." );
            }
        }

        LogicalMaterializedView view = catalog.getLogicalRel( namespaceId ).addMaterializedView(
                viewName,
                namespaceId,
                algRoot.alg,
                algRoot.collation,
                underlying,
                fieldList,
                materializedCriteria,
                query,
                language,
                ordered
        );

        // Creates a list with all columns, tableId is needed to create the primary key
        List<FieldInformation> fields = getColumnInformation( projectedColumns, fieldList, true, view.id );

        Map<String, LogicalColumn> ids = new LinkedHashMap<>();

        for ( FieldInformation field : fields ) {
            ids.put( field.name, addColumn( namespaceId, field.name, field.typeInformation, field.collation, field.defaultValue, view.id, field.position ) );
        }

        // Sets previously created primary key
        long pkId = ids.get( fields.get( fields.size() - 1 ).name ).id;
        catalog.getLogicalRel( namespaceId ).addPrimaryKey( view.id, List.of( pkId ) );

        AllocationPartitionGroup group = catalog.getAllocRel( namespaceId ).addPartitionGroup( view.id, UNPARTITIONED, namespaceId, PartitionType.NONE, 1, false );
        AllocationPartition partition = catalog.getAllocRel( namespaceId ).addPartition( view.id, namespaceId, group.id, null, false, PlacementType.AUTOMATIC, DataPlacementRole.UP_TO_DATE, null, PartitionType.NONE );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocRel( namespaceId ).addPlacement( view.id, namespaceId, store.adapterId );

            addAllocationsForPlacement( namespaceId, statement, view, placement.id, List.copyOf( ids.values() ), List.of( pkId ), List.of( partition.id ), store );
        }
        addBlankPartition( namespaceId, view.id, List.of( group.id ), List.of( partition.id ) );

        catalog.updateSnapshot();

        // Selected data from tables is added into the newly crated materialized view
        MaterializedViewManager materializedManager = MaterializedViewManager.getInstance();
        materializedManager.addData( statement.getTransaction(), stores, algRoot, view );
    }


    private void checkModelLangCompatibility( DataModel model, long namespaceId ) {
        LogicalNamespace namespace = catalog.getSnapshot().getNamespace( namespaceId ).orElseThrow();
        if ( namespace.dataModel != model ) {
            throw new GenericRuntimeException(
                    "The used language cannot execute schema changing queries on this entity with the data model %s.",
                    namespace.getDataModel() );
        }
    }


    @Override
    public void refreshView( Statement statement, Long materializedId ) {
        MaterializedViewManager materializedManager = MaterializedViewManager.getInstance();
        materializedManager.updateData( statement.getTransaction(), materializedId );
        materializedManager.updateMaterializedTime( materializedId );
    }


    @Override
    public long createGraph( String name, boolean modifiable, @Nullable List<DataStore<?>> stores, boolean ifNotExists, boolean replace, boolean caseSensitive, Statement statement ) {
        assert !replace : "Graphs cannot be replaced yet.";
        String adjustedName = caseSensitive ? name : name.toLowerCase();

        if ( stores == null ) {
            // Ask router on which storeId(s) the graph should be placed
            stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewEntity();
        }

        // add general graph
        long graphId = catalog.createNamespace( adjustedName, DataModel.GRAPH, caseSensitive );

        // add specialized graph
        LogicalGraph logical = catalog.getLogicalGraph( graphId ).addGraph( graphId, adjustedName, modifiable );

        catalog.updateSnapshot();

        AllocationPartition partition = catalog.getAllocGraph( graphId ).addPartition( logical, PartitionType.NONE, "undefined" );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocGraph( graphId ).addPlacement( logical, store.adapterId );
            AllocationGraph alloc = catalog.getAllocGraph( graphId ).addAllocation( logical, placement.id, partition.id, store.getAdapterId() );

            store.createGraph( statement.getPrepareContext(), logical, alloc );
        }

        catalog.updateSnapshot();

        return graphId;
    }


    @Override
    public long createGraphPlacement( long graphId, List<DataStore<?>> stores, Statement statement ) {

        LogicalGraph graph = catalog.getSnapshot().graph().getGraph( graphId ).orElseThrow();
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        List<Long> preExistingPlacements = snapshot
                .alloc()
                .getFromLogical( graphId )
                .stream()
                .filter( p -> !stores.stream().map( Adapter::getAdapterId ).toList().contains( p.adapterId ) )
                .map( p -> p.adapterId )
                .toList();

        List<AllocationPartition> partitions = catalog.getSnapshot().alloc().getPartitionsFromLogical( graphId );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocGraph( graphId ).addPlacement( graph, store.adapterId );
            AllocationGraph alloc = catalog.getAllocGraph( graphId ).addAllocation( graph, placement.id, partitions.get( 0 ).id, store.getAdapterId() );

            store.createGraph( statement.getPrepareContext(), graph, alloc );

            if ( !preExistingPlacements.isEmpty() ) {
                // Copy the data to the newly added column placements
                DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
                dataMigrator.copyGraphData( alloc, graph, statement.getTransaction() );
            }

        }

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

        return graphId;
    }


    @Override
    public void dropGraphPlacement( long graphId, DataStore<?> store, Statement statement ) {
        AllocationPlacement placement = statement.getTransaction().getSnapshot().alloc().getPlacement( store.getAdapterId(), graphId ).orElseThrow();

        List<AllocationPartition> partitions = statement.getTransaction().getSnapshot().alloc().getPartitionsFromLogical( graphId );

        for ( AllocationPartition partition : partitions ) {
            Optional<AllocationEntity> optAlloc = statement.getTransaction().getSnapshot().alloc().getAlloc( placement.id, partition.id );
            if ( optAlloc.isEmpty() ) {
                // this partition is not placed on this storeId
                continue;
            }
            AllocationGraph alloc = optAlloc.get().unwrap( AllocationGraph.class ).orElseThrow();

            store.dropGraph( statement.getPrepareContext(), alloc );

            catalog.getAllocGraph( graphId ).deleteAllocation( alloc.id );

            catalog.getAllocGraph( graphId ).removePlacement( alloc.placementId );
        }

        statement.getQueryProcessor().resetCaches();

    }


    @Override
    public void createGraphAlias( long graphId, String alias, boolean ifNotExists ) {
        catalog.getLogicalGraph( graphId ).addGraphAlias( graphId, alias, ifNotExists );
    }


    @Override
    public void dropGraphAlias( long graphId, String alias, boolean ifNotExists ) {
        alias = alias.toLowerCase();
        catalog.getLogicalGraph( graphId ).removeGraphAlias( graphId, alias, ifNotExists );
    }


    @Override
    public void replaceGraphAlias( long graphId, String oldAlias, String alias ) {
        alias = alias.toLowerCase();
        oldAlias = oldAlias.toLowerCase();
        catalog.getLogicalGraph( graphId ).removeGraphAlias( graphId, oldAlias, true );
        catalog.getLogicalGraph( graphId ).addGraphAlias( graphId, alias, true );
    }


    @Override
    public void dropGraph( long graphId, boolean ifExists, Statement statement ) {
        Optional<LogicalGraph> optionalGraph = catalog.getSnapshot().graph().getGraph( graphId );

        if ( optionalGraph.isEmpty() ) {
            if ( !ifExists ) {
                throw new GenericRuntimeException( "There exists no graph with id %s", graphId );
            }
            return;
        }
        AllocSnapshot allocSnapshot = catalog.getSnapshot().alloc();
        for ( AllocationEntity alloc : allocSnapshot.getFromLogical( graphId ) ) {
            AdapterManager.getInstance()
                    .getStore( alloc.adapterId )
                    .orElseThrow()
                    .dropGraph( statement.getPrepareContext(), alloc.unwrap( AllocationGraph.class ).orElseThrow() );
            catalog.getAllocGraph( alloc.namespaceId ).deleteAllocation( alloc.id );
            catalog.getAllocGraph( alloc.namespaceId ).removePlacement( alloc.placementId );
        }

        catalog.getAllocGraph( graphId ).removePartition( catalog.getSnapshot().alloc().getPartitionsFromLogical( graphId ).get( 0 ).id );
        catalog.getLogicalGraph( graphId ).deleteGraph( graphId );

        catalog.dropNamespace( graphId );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private List<FieldInformation> getColumnInformation( List<String> projectedColumns, AlgDataType fieldList ) {
        return getColumnInformation( projectedColumns, fieldList, false, 0 );
    }


    private List<FieldInformation> getColumnInformation( List<String> projectedColumns, AlgDataType fieldList, boolean addPrimary, long tableId ) {
        List<FieldInformation> columns = new ArrayList<>();

        int position = 1;
        for ( AlgDataTypeField alg : fieldList.getFields() ) {
            AlgDataType type = alg.getType();
            if ( alg.getType().getPolyType() == PolyType.ARRAY ) {
                type = alg.getType().getComponentType();
            }
            String colName = alg.getName();
            if ( projectedColumns != null ) {
                colName = projectedColumns.get( position - 1 );
            }

            columns.add( new FieldInformation(
                    colName.toLowerCase().replaceAll( "[^A-Za-z0-9]", "_" ),
                    new ColumnTypeInformation(
                            type.getPolyType(),
                            alg.getType().getPolyType(),
                            type.getRawPrecision(),
                            type.getScale(),
                            alg.getType().getPolyType() == PolyType.ARRAY ? (int) ((ArrayType) alg.getType()).getDimension() : -1,
                            alg.getType().getPolyType() == PolyType.ARRAY ? (int) ((ArrayType) alg.getType()).getCardinality() : -1,
                            alg.getType().isNullable() ),
                    Collation.getDefaultCollation(),
                    null,
                    position ) );
            position++;

        }

        if ( addPrimary ) {
            String primaryName = "_matid_" + tableId;
            columns.add( new FieldInformation(
                    primaryName,
                    new ColumnTypeInformation(
                            PolyType.INTEGER,
                            PolyType.INTEGER,
                            -1,
                            -1,
                            -1,
                            -1,
                            false ),
                    Collation.getDefaultCollation(),
                    null,
                    position ) );
        }

        return columns;
    }


    private Map<Long, List<Long>> findUnderlyingTablesOfView( AlgNode algNode, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList ) {
        if ( algNode instanceof LogicalRelScan ) {
            List<Long> underlyingColumns = getUnderlyingColumns( algNode, fieldList );
            underlyingTables.put( algNode.getEntity().id, underlyingColumns );
        } else if ( algNode instanceof LogicalRelViewScan ) {
            List<Long> underlyingColumns = getUnderlyingColumns( algNode, fieldList );
            underlyingTables.put( algNode.getEntity().id, underlyingColumns );
        }
        if ( algNode instanceof BiAlg ) {
            findUnderlyingTablesOfView( ((BiAlg) algNode).getLeft(), underlyingTables, fieldList );
            findUnderlyingTablesOfView( ((BiAlg) algNode).getRight(), underlyingTables, fieldList );
        } else if ( algNode instanceof SingleAlg ) {
            findUnderlyingTablesOfView( ((SingleAlg) algNode).getInput(), underlyingTables, fieldList );
        }
        return underlyingTables;
    }


    private List<Long> getUnderlyingColumns( AlgNode algNode, AlgDataType fieldList ) {
        LogicalTable table = algNode.getEntity().unwrap( LogicalTable.class ).orElseThrow();
        List<LogicalColumn> columns = Catalog.getInstance().getSnapshot().rel().getColumns( table.id );
        List<String> logicalColumnNames = columns.stream().map( c -> c.name ).toList();
        List<Long> underlyingColumns = new ArrayList<>();
        for ( int i = 0; i < columns.size(); i++ ) {
            for ( AlgDataTypeField algDataTypeField : fieldList.getFields() ) {
                String name = logicalColumnNames.get( i );
                if ( algDataTypeField.getName().equals( name ) ) {
                    underlyingColumns.add( columns.get( i ).id );
                }
            }
        }
        return underlyingColumns;
    }


    @Override
    public void createTable( long namespaceId, String name, List<FieldInformation> fields, List<ConstraintInformation> constraints, boolean ifNotExists, @Nullable List<DataStore<?>> stores, PlacementType placementType, Statement statement ) {
        String adjustedName = adjustNameIfNeeded( name, namespaceId );

        // Check if there is already a table with this name
        if ( assertEntityExists( namespaceId, adjustedName, ifNotExists ) ) {
            return;
        }

        if ( stores == null ) {
            // Ask router on which storeId(s) the table should be placed
            stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewEntity();
        }

        // addLTable
        LogicalTable logical = catalog.getLogicalRel( namespaceId ).addTable(
                adjustedName,
                EntityType.ENTITY,
                true );

        // addLColumns

        Map<String, LogicalColumn> ids = new HashMap<>();
        for ( FieldInformation information : fields ) {
            ids.put( information.name, addColumn( namespaceId, information.name, information.typeInformation, information.collation, information.defaultValue, logical.id, information.position ) );
        }

        List<Long> pkIds = new ArrayList<>();

        // create foreign keys later on
        for ( ConstraintInformation constraint : constraints.stream().filter( c -> c.getType() != ConstraintType.FOREIGN ).toList() ) {
            List<Long> columnIds = constraint.columnNames.stream().map( key -> ids.get( key ).id ).toList();
            createConstraint( constraint, namespaceId, columnIds, logical.id );

            if ( constraint.type == ConstraintType.PRIMARY ) {
                pkIds = columnIds;
            }
        }
        if ( constraints.stream().noneMatch( c -> c.type == ConstraintType.PRIMARY ) ) {
            // no primary was set for now, we attach condition to check on commit
            catalog.attachCommitConstraint(
                    () -> logical.primaryKey != null && catalog.getSnapshot().rel().getPrimaryKey( logical.primaryKey ).isPresent(),
                    "No primary key defined for table: " + name );
        }

        // addATable
        AllocationPartition partition = createSinglePartition( namespaceId, logical ).left;

        List<LogicalColumn> columns = ids.values().stream().sorted( Comparator.comparingInt( c -> c.position ) ).toList();

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocRel( namespaceId ).addPlacement( logical.id, namespaceId, store.adapterId );

            addAllocationsForPlacement( namespaceId, statement, logical, placement.id, columns, pkIds, List.of( partition.id ), store );
        }

        catalog.updateSnapshot();

        constraints.stream().filter( c -> c.getType() == ConstraintType.FOREIGN ).forEach( c -> {
            List<Long> columnIds = c.columnNames.stream().map( key -> ids.get( key ).id ).toList();
            createConstraint( c, namespaceId, columnIds, logical.id );
        } );

        catalog.updateSnapshot();
    }


    @NotNull
    private Pair<AllocationPartition, PartitionProperty> createSinglePartition( long namespaceId, LogicalTable logical ) {
        AllocationPartitionGroup group = catalog.getAllocRel( namespaceId ).addPartitionGroup( logical.id, UNPARTITIONED, namespaceId, PartitionType.NONE, 1, false );
        AllocationPartition partition = catalog.getAllocRel( namespaceId ).addPartition( logical.id, namespaceId, group.id, null, false, PlacementType.AUTOMATIC, DataPlacementRole.REFRESHABLE, null, PartitionType.NONE );
        PartitionProperty property = addBlankPartition( namespaceId, logical.id, List.of( group.id ), List.of( partition.id ) );
        return Pair.of( partition, property );
    }


    @SuppressWarnings("UnusedReturnValue")
    private List<AllocationTable> addAllocationsForPlacement( long namespaceId, Statement statement, LogicalTable logical, long placementId, List<LogicalColumn> lColumns, List<Long> pkIds, List<Long> partitionIds, Adapter<?> adapter ) {
        List<AllocationColumn> columns = new ArrayList<>();
        int i = 0;
        for ( LogicalColumn column : sortByPosition( lColumns ) ) {
            columns.add( catalog.getAllocRel( namespaceId ).addColumn( placementId, logical.id, column.id, adapter.adapterId, PlacementType.AUTOMATIC, i++ ) );
        }

        buildNamespace( namespaceId, logical, adapter );
        List<AllocationTable> tables = new ArrayList<>();
        for ( Long partitionId : partitionIds ) {
            tables.add( addAllocationTable( namespaceId, statement, logical, lColumns, pkIds, placementId, partitionId, columns, adapter ) );
        }
        return tables;
    }


    private PartitionProperty addBlankPartition( long namespaceId, long logicalEntityId, List<Long> groupIds, List<Long> partitionIds ) {
        PartitionProperty partitionProperty = PartitionProperty.builder()
                .entityId( logicalEntityId )
                .partitionType( PartitionType.NONE )
                .isPartitioned( false )
                .partitionGroupIds( ImmutableList.copyOf( groupIds ) )
                .partitionIds( ImmutableList.copyOf( partitionIds ) )
                .reliesOnPeriodicChecks( false )
                .build();

        catalog.getAllocRel( namespaceId ).addPartitionProperty( logicalEntityId, partitionProperty );
        return partitionProperty;
    }


    private AllocationTable addAllocationTable( long namespaceId, Statement statement, LogicalTable logical, List<LogicalColumn> lColumns, List<Long> pkIds, long placementId, long partitionId, List<AllocationColumn> aColumns, Adapter<?> adapter ) {
        AllocationTable alloc = catalog.getAllocRel( namespaceId ).addAllocation( adapter.adapterId, placementId, partitionId, logical.id );

        adapter.createTable( statement.getPrepareContext(), LogicalTableWrapper.of( logical, sortByPosition( lColumns ), pkIds ), AllocationTableWrapper.of( alloc, aColumns ) );
        return alloc;
    }


    @NotNull
    private static List<LogicalColumn> sortByPosition( List<LogicalColumn> columns ) {
        return columns.stream().sorted( Comparator.comparingInt( a -> a.position ) ).toList();
    }


    private void buildNamespace( long namespaceId, LogicalTable logical, Adapter<?> store ) {
        store.updateNamespace( logical.getNamespaceName(), namespaceId );
    }


    @Override
    public void createCollection( long namespaceId, String name, boolean ifNotExists, List<DataStore<?>> stores, PlacementType placementType, Statement statement ) {
        String adjustedName = adjustNameIfNeeded( name, namespaceId );

        checkModelLangCompatibility( DataModel.DOCUMENT, namespaceId );

        if ( assertEntityExists( namespaceId, adjustedName, ifNotExists ) ) {
            return;
        }

        if ( stores == null ) {
            // Ask router on which storeId(s) the table should be placed
            stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewEntity();
        }

        // addLTable
        LogicalCollection logical = catalog.getLogicalDoc( namespaceId ).addCollection(
                adjustedName,
                EntityType.ENTITY,
                true );

        AllocationPartition partition = catalog.getAllocDoc( namespaceId ).addPartition( logical, PartitionType.NONE, "undefined" );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocDoc( namespaceId ).addPlacement( logical, store.adapterId );
            AllocationCollection alloc = catalog.getAllocDoc( namespaceId ).addAllocation( logical, placement.id, partition.id, store.getAdapterId() );

            store.createCollection( statement.getPrepareContext(), logical, alloc );
        }

        Catalog.getInstance().updateSnapshot();

    }


    private boolean assertEntityExists( long namespaceId, String name, boolean ifNotExists ) {
        Snapshot snapshot = catalog.getSnapshot();
        LogicalNamespace namespace = snapshot.getNamespace( namespaceId ).orElseThrow();
        // Check if there is already an entity with this name
        if ( (namespace.dataModel == DataModel.RELATIONAL && snapshot.rel().getTable( namespaceId, name ).isPresent())
                || (namespace.dataModel == DataModel.DOCUMENT && snapshot.doc().getCollection( namespaceId, name ).isPresent())
                || (namespace.dataModel == DataModel.GRAPH && snapshot.graph().getGraph( namespaceId ).isPresent()) ) {
            if ( ifNotExists ) {
                // It is ok that there is already a table with this name because "IF NOT EXISTS" was specified
                return true;
            } else {
                throw new GenericRuntimeException( "There already exists an entity with the name %s", name );
            }
        }
        return false;
    }


    @Override
    public void dropCollection( LogicalCollection collection, Statement statement ) {
        Snapshot snapshot = catalog.getSnapshot();

        AdapterManager manager = AdapterManager.getInstance();

        List<AllocationEntity> allocations = snapshot.alloc().getFromLogical( collection.id );
        for ( AllocationEntity allocation : allocations ) {
            manager.getStore( allocation.adapterId ).orElseThrow().dropCollection( statement.getPrepareContext(), allocation.unwrap( AllocationCollection.class ).orElseThrow() );

            catalog.getAllocDoc( allocation.namespaceId ).removeAllocation( allocation.id );
            catalog.getAllocDoc( allocation.namespaceId ).removePlacement( allocation.placementId );
        }
        catalog.getAllocDoc( collection.namespaceId ).removePartition( snapshot.alloc().getPartitionsFromLogical( collection.id ).get( 0 ).id );
        catalog.getLogicalDoc( collection.namespaceId ).deleteCollection( collection.id );

        catalog.updateSnapshot();

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void createCollectionPlacement( long namespaceId, String name, List<DataStore<?>> stores, Statement statement ) {
        LogicalCollection collection = catalog.getSnapshot().doc().getCollection( namespaceId, name ).orElseThrow();

        // Initially create DataPlacement containers on every storeId the table should be placed.

        List<Long> preExistingPlacements = catalog.getSnapshot()
                .alloc()
                .getFromLogical( collection.id )
                .stream()
                .filter( p -> !stores.stream().map( Adapter::getAdapterId ).toList().contains( p.adapterId ) )
                .map( p -> p.adapterId )
                .toList();

        List<AllocationPartition> partitions = catalog.getSnapshot().alloc().getPartitionsFromLogical( collection.id );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocDoc( collection.namespaceId ).addPlacement( collection, store.adapterId );
            AllocationCollection alloc = catalog.getAllocDoc( collection.namespaceId ).addAllocation(
                    collection,
                    placement.id,
                    partitions.get( 0 ).id,
                    store.getAdapterId() );

            store.createCollection( statement.getPrepareContext(), collection, alloc );

            catalog.updateSnapshot();

            if ( !preExistingPlacements.isEmpty() ) {
                // Copy the data to the newly added column placements
                DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
                dataMigrator.copyDocData( alloc, collection, statement.getTransaction() );
            }
        }
    }


    @Override
    public void dropCollectionPlacement( long namespaceId, LogicalCollection collection, List<DataStore<?>> dataStores, Statement statement ) {
        for ( DataStore<?> store : dataStores ) {

            AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( store.getAdapterId(), collection.id ).orElseThrow();

            for ( AllocationPartition partition : catalog.getSnapshot().alloc().getPartitionsFromLogical( collection.id ) ) {
                Optional<AllocationEntity> optAlloc = catalog.getSnapshot().alloc().getAlloc( placement.id, partition.id );
                if ( optAlloc.isEmpty() ) {
                    // this partition is not placed on this storeId
                    continue;
                }
                AllocationCollection alloc = optAlloc.get().unwrap( AllocationCollection.class ).orElseThrow();

                store.dropCollection( statement.getPrepareContext(), alloc );

                catalog.getAllocDoc( namespaceId ).removeAllocation( alloc.id );

                catalog.getAllocDoc( namespaceId ).removePlacement( alloc.placementId );
            }

        }

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

    }


    @Override
    public void createTablePartition( PartitionInformation partitionInfo, List<DataStore<?>> stores, Statement statement ) throws TransactionException {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        PartitionProperty initialProperty = snapshot.alloc().getPartitionProperty( partitionInfo.table.id ).orElseThrow();
        Pair<List<AllocationPartition>, PartitionProperty> result = addGroupsAndPartitions( partitionInfo, snapshot );

        LogicalTable unPartitionedTable = partitionInfo.table;
        LogicalColumn partitionColumn = snapshot.rel().getColumn( partitionInfo.table.id, partitionInfo.columnName ).orElseThrow();

        // Update catalog table
        catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionProperty(
                partitionInfo.table.id,
                result.right );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = partitionInfo.table.primaryKey;
        LogicalRelSnapshot relSnapshot = catalog.getSnapshot().rel();
        List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).orElseThrow().fieldIds;

        // This gets us only one ccp per storeId (first part of PK)
        boolean fillStores = false;
        if ( stores == null ) {
            stores = new ArrayList<>();
            fillStores = true;
        }

        // Now get the partitioned table, partitionInfo still contains the basic/unpartitioned table.
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();

        List<AllocationPlacement> placements = snapshot.alloc().getPlacementsFromLogical( unPartitionedTable.id );
        Map<AllocationPlacement, List<AllocationTable>> newAllocations = new HashMap<>();

        // add all allocations which we could use to "find" our data, as we create new partitions we exclude allocations with non-old partitions
        List<AllocationEntity> sourceAllocs = new ArrayList<>( catalog.getSnapshot().alloc().getFromLogical( unPartitionedTable.id ).stream().filter( a -> initialProperty.partitionIds.contains( a.partitionId ) ).toList() );

        for ( AllocationPlacement placement : placements ) {
            if ( !fillStores ) {
                continue;
            }
            // Ask router on which storeId(s) the table should be placed
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId ).orElseThrow();
            if ( !(adapter instanceof DataStore<?> store) ) {
                continue;
            }
            stores.add( store );

            List<AllocationColumn> columns = catalog.getSnapshot().alloc().getColumns( placement.id );
            List<AllocationTable> partitionAllocations = new ArrayList<>();
            List<LogicalColumn> logicalColumns = columns.stream().map( c -> catalog.getSnapshot().rel().getColumn( c.columnId ).orElseThrow() ).toList();
            for ( AllocationPartition partition : result.left ) {

                partitionAllocations.add( addAllocationTable( partitionInfo.table.namespaceId, statement, unPartitionedTable, logicalColumns, pkColumnIds, placement.id, partition.id, columns, store ) );
            }
            newAllocations.put( placement, partitionAllocations );

            // Copy data from the old partition to new partitions
            catalog.updateSnapshot();
            dataMigrator.copyAllocationData(
                    statement.getTransaction(),
                    catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(),
                    sourceAllocs.stream().map( s -> s.unwrap( AllocationTable.class ).orElseThrow() ).toList(),
                    result.right,
                    newAllocations.get( placement ),
                    unPartitionedTable );
        }

        // Adjust indexes
        List<LogicalIndex> indexes = relSnapshot.getIndexes( unPartitionedTable.id, false );
        for ( LogicalIndex index : indexes ) {
            // Remove old index
            DataStore<?> ds = AdapterManager.getInstance().getStore( index.location ).orElseThrow();
            ds.dropIndex( statement.getPrepareContext(), index, result.right.partitionIds );
            catalog.getLogicalRel( partitionInfo.table.namespaceId ).deleteIndex( index.id );
            // Add new index
            LogicalIndex newIndex = catalog.getLogicalRel( partitionInfo.table.namespaceId ).addIndex(
                    unPartitionedTable.id,
                    index.key.fieldIds,
                    index.unique,
                    index.method,
                    index.methodDisplayName,
                    index.location,
                    index.type,
                    index.name );
            if ( index.location < 0 ) {
                IndexManager.getInstance().addIndex( newIndex, statement );
            } else {
                String physicalName = ds.addIndex(
                        statement.getPrepareContext(),
                        index, newAllocations.entrySet().stream().filter( e -> e.getKey().adapterId == ds.adapterId ).findFirst().orElseThrow().getValue() );//catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( ds.getAdapterId(), unPartitionedTable.id ) );
                catalog.getLogicalRel( partitionInfo.table.namespaceId ).setIndexPhysicalName( index.id, physicalName );
            }
        }

        // Remove old tables
        sourceAllocs.forEach( s -> deleteAllocation( statement, s ) );

        catalog.getAllocRel( partitionInfo.table.namespaceId ).deletePartitionGroup( snapshot.alloc().getPartitionProperty( unPartitionedTable.id ).orElseThrow().partitionIds.get( 0 ) );
        initialProperty.partitionIds.forEach( id -> catalog.getAllocRel( partitionInfo.table.namespaceId ).deletePartition( id ) );

        catalog.updateSnapshot();
        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private Pair<List<AllocationPartition>, PartitionProperty> addGroupsAndPartitions( PartitionInformation partitionInfo, Snapshot snapshot ) {
        LogicalColumn logicalColumn = snapshot.rel().getColumn( partitionInfo.table.id, partitionInfo.columnName ).orElseThrow();

        PartitionType actualPartitionType = PartitionType.getByName( partitionInfo.typeName );

        // Convert partition names and check whether they are unique
        List<String> sanitizedPartitionGroupNames = partitionInfo.partitionGroupNames
                .stream()
                .map( name -> name.trim().toLowerCase() )
                .toList();
        if ( sanitizedPartitionGroupNames.size() != new HashSet<>( sanitizedPartitionGroupNames ).size() ) {
            throw new GenericRuntimeException( "Name is not unique" );
        }

        // Check if specified partitionColumn is even part of the table
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partition group for table: {} with id {} on column: {}", partitionInfo.table.name, partitionInfo.table.id, logicalColumn.id );
        }

        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( actualPartitionType );

        // Check whether partition function supports type of partition column
        if ( !partitionManager.supportsColumnOfType( logicalColumn.type ) ) {
            throw new GenericRuntimeException( "The partition function %s does not support columns of type %s", actualPartitionType, logicalColumn.type );
        }

        int numberOfPartitionGroups = partitionInfo.numberOfPartitionGroups;
        // Calculate how many partitions exist if partitioning is applied.
        if ( partitionInfo.partitionGroupNames.size() >= 2 && partitionInfo.numberOfPartitionGroups == 0 ) {
            numberOfPartitionGroups = partitionInfo.partitionGroupNames.size();
        }

        int numberOfPartitions = partitionInfo.numberOfPartitions;
        int numberOfPartitionsPerGroup = partitionManager.getNumberOfPartitionsPerGroup( numberOfPartitions );

        if ( partitionManager.requiresUnboundPartitionGroup() ) {
            // Because of the implicit unbound partition
            numberOfPartitionGroups = partitionInfo.partitionGroupNames.size();
            numberOfPartitionGroups += 1;
        }

        // Validate & adjust partition setup
        partitionInfo = partitionInfo.toBuilder().qualifiers( partitionManager.validateAdjustPartitionGroupSetup( partitionInfo.qualifiers, numberOfPartitionGroups, partitionInfo.partitionGroupNames, logicalColumn ) ).build();

        // Loop over value to create those partitions with partitionKey to uniquelyIdentify partition
        Map<AllocationPartitionGroup, List<AllocationPartition>> partitionGroups = new HashMap<>();
        for ( int i = 0; i < numberOfPartitionGroups; i++ ) {
            String partitionGroupName;
            AllocationPartitionGroup group;

            // Make last partition unbound partition
            if ( partitionManager.requiresUnboundPartitionGroup() && i == numberOfPartitionGroups - 1 ) {
                group = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                        partitionInfo.table.id,
                        "Unbound",
                        partitionInfo.table.namespaceId,
                        actualPartitionType,
                        numberOfPartitionsPerGroup,
                        true );
            } else {
                // If no names have been explicitly defined
                partitionGroupName = "part_" + i;
                if ( !partitionInfo.partitionGroupNames.isEmpty() ) {
                    partitionGroupName = partitionInfo.partitionGroupNames.get( i );
                }

                // Mainly needed for HASH
                group = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                        partitionInfo.table.id,
                        partitionGroupName,
                        partitionInfo.table.namespaceId,
                        actualPartitionType,
                        numberOfPartitionsPerGroup,
                        false );
            }
            List<AllocationPartition> partitions = new ArrayList<>();
            partitionGroups.put( group, partitions );
        }

        int j = 0;
        for ( AllocationPartitionGroup group : partitionGroups.keySet() ) {
            List<String> qualifiers = group.isUnbound ? null : (j < partitionInfo.qualifiers.size() ? partitionInfo.qualifiers.get( j++ ) : null);
            partitionGroups.put( group, List.of( catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition(
                    partitionInfo.table.id,
                    partitionInfo.table.namespaceId,
                    group.id,
                    group.name,
                    group.isUnbound,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.REFRESHABLE,
                    qualifiers,
                    PartitionType.NONE ) ) );
        }

        //get All PartitionGroups and then get all partitionIds  for each PG and add them to completeList of partitionIds
        List<AllocationPartition> partitions = partitionGroups.values().stream().flatMap( Collection::stream ).collect( Collectors.toList() );

        PartitionProperty partitionProperty;
        if ( actualPartitionType == PartitionType.TEMPERATURE ) {
            partitionProperty = handleTemperaturePartitioning( partitionInfo, numberOfPartitions, partitionGroups, partitions, logicalColumn, actualPartitionType );
        } else {
            partitionProperty = PartitionProperty.builder()
                    .entityId( logicalColumn.tableId )
                    .partitionType( actualPartitionType )
                    .isPartitioned( true )
                    .partitionColumnId( logicalColumn.id )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroups.keySet().stream().map( g -> g.id ).toList() ) )
                    .partitionIds( ImmutableList.copyOf( partitions.stream().map( p -> p.id ).toList() ) )
                    .reliesOnPeriodicChecks( false )
                    .build();
        }
        return Pair.of( partitions, partitionProperty );
    }


    private PartitionProperty handleTemperaturePartitioning( PartitionInformation partitionInfo, int numberOfPartitions, Map<AllocationPartitionGroup, List<AllocationPartition>> partitionGroups, List<AllocationPartition> partitions, LogicalColumn logicalColumn, PartitionType actualPartitionType ) {
        PartitionProperty partitionProperty;
        long frequencyInterval = ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInterval();
        frequencyInterval = switch ( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getIntervalUnit().toString() ) {
            case "days" -> frequencyInterval * 60 * 60 * 24;
            case "hours" -> frequencyInterval * 60 * 60;
            case "minutes" -> frequencyInterval * 60;
            default -> frequencyInterval;
        };

        int hotPercentageIn = Integer.parseInt( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageIn().toString() );
        int hotPercentageOut = Integer.parseInt( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageOut().toString() );

        //Initially distribute partitions as intended in a running system
        long numberOfPartitionsInHot = (long) numberOfPartitions * hotPercentageIn / 100;
        if ( numberOfPartitionsInHot == 0 ) {
            numberOfPartitionsInHot = 1;
        }

        long numberOfPartitionsInCold = numberOfPartitions - numberOfPartitionsInHot;

        // -1 because one partition is already created in HOT
        AllocationPartitionGroup firstGroup = partitionGroups.keySet().stream().findFirst().orElseThrow();

        // -1 because one partition is already created in HOT
        for ( int i = 0; i < numberOfPartitionsInHot - 1; i++ ) {
            partitions.add( catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition(
                    partitionInfo.table.id,
                    partitionInfo.table.namespaceId,
                    firstGroup.id,
                    null,
                    false,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.UP_TO_DATE,
                    null, PartitionType.NONE ) );
        }

        // -1 because one partition is already created in COLD
        AllocationPartitionGroup secondGroup = new ArrayList<>( partitionGroups.keySet() ).get( 1 );

        for ( int i = 0; i < numberOfPartitionsInCold - 1; i++ ) {
            partitions.add( catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition(
                    partitionInfo.table.id,
                    partitionInfo.table.namespaceId,
                    secondGroup.id,
                    null,
                    false,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.UP_TO_DATE,
                    null, PartitionType.NONE ) );
        }

        partitionProperty = TemperaturePartitionProperty.builder()
                .entityId( logicalColumn.tableId )
                .partitionType( actualPartitionType )
                .isPartitioned( true )
                .internalPartitionFunction( PartitionType.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInternalPartitionFunction().toString().toUpperCase() ) )
                .partitionColumnId( logicalColumn.id )
                .partitionGroupIds( ImmutableList.copyOf( partitionGroups.keySet().stream().map( g -> g.id ).collect( Collectors.toList() ) ) )
                .partitionIds( ImmutableList.copyOf( partitions.stream().map( p -> p.id ).toList() ) )
                .partitionCostIndication( PartitionCostIndication.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getAccessPattern().toString().toUpperCase() ) )
                .frequencyInterval( frequencyInterval )
                .hotAccessPercentageIn( hotPercentageIn )
                .hotAccessPercentageOut( hotPercentageOut )
                .reliesOnPeriodicChecks( true )
                .hotPartitionGroupId( firstGroup.id )
                .coldPartitionGroupId( secondGroup.id )
                .numPartitions( partitions.size() )
                .numPartitionGroups( partitionGroups.size() )
                .build();
        return partitionProperty;
    }


    @Override
    public void dropTablePartition( LogicalTable table, Statement statement ) throws TransactionException {
        long tableId = table.id;
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        if ( log.isDebugEnabled() ) {
            log.debug( "Merging partitions for table: {} with id {} on schema: {}",
                    table.name, table.id, snapshot.getNamespace( table.namespaceId ) );
        }

        LogicalRelSnapshot relSnapshot = catalog.getSnapshot().rel();

        PartitionProperty property = snapshot.alloc().getPartitionProperty( table.id ).orElseThrow();

        // Need to gather the partitionDistribution before actually merging
        // We need a columnPlacement for every partition

        // Update catalog table
        catalog.getAllocRel( table.namespaceId ).deleteProperty( tableId );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = table.primaryKey;
        List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).orElseThrow().fieldIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        // This gets us only one ccp per storeId (first part of PK)

        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        List<LogicalColumn> logicalColumns = snapshot.rel().getColumns( tableId );

        Pair<AllocationPartition, PartitionProperty> partitionProperty = createSinglePartition( table.namespaceId, table );
        Set<AllocationEntity> sources = new HashSet<>();
        // For merge create only full placements on the used stores. Otherwise, partition constraints might not hold
        for ( AllocationPlacement placement : catalog.getSnapshot().alloc().getPlacementsFromLogical( tableId ) ) {

            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId ).orElseThrow();
            if ( !(adapter instanceof DataStore<?> store) ) {
                continue;
            }

            List<AllocationTable> sourceTables = new ArrayList<>( snapshot.alloc().getAllocsOfPlacement( placement.id ).stream().map( t -> t.unwrap( AllocationTable.class ).orElseThrow() ).toList() );
            List<Long> missingPartitions = new ArrayList<>( property.partitionIds );
            missingPartitions.removeAll( sourceTables.stream().map( s -> s.partitionId ).toList() );

            sources.addAll( sourceTables );

            for ( long missingPartition : missingPartitions ) {
                sourceTables.addAll( snapshot.alloc().getAllocsOfPartitions( missingPartition ).stream().filter( a -> a.adapterId != store.adapterId ).map( a -> a.unwrap( AllocationTable.class ).orElseThrow() ).toList() );
            }

            List<AllocationColumn> columns = snapshot.alloc().getColumns( placement.id );

            // First create new tables
            AllocationTable targetTable = addAllocationTable( table.namespaceId, statement, table, logicalColumns, pkColumnIds, placement.id, partitionProperty.left.id, columns, store );

            catalog.updateSnapshot();
            dataMigrator.copyAllocationData(
                    statement.getTransaction(),
                    catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(),
                    sourceTables,
                    partitionProperty.right,
                    List.of( targetTable ),
                    table );


        }

        // Adjust indexes
        List<LogicalIndex> indexes = relSnapshot.getIndexes( table.id, false );
        for ( LogicalIndex index : indexes ) {
            // Remove old index
            DataStore<?> ds = AdapterManager.getInstance().getStore( index.location ).orElseThrow();
            ds.dropIndex( statement.getPrepareContext(), index, property.partitionIds );
            catalog.getLogicalRel( table.namespaceId ).deleteIndex( index.id );
            // Add new index
            LogicalIndex newIndex = catalog.getLogicalRel( table.namespaceId ).addIndex(
                    table.id,
                    index.key.fieldIds,
                    index.unique,
                    index.method,
                    index.methodDisplayName,
                    index.location,
                    index.type,
                    index.name );
            if ( index.location < 0 ) {
                IndexManager.getInstance().addIndex( newIndex, statement );
            } else {
                AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( ds.adapterId, tableId ).orElseThrow();
                ds.addIndex(
                        statement.getPrepareContext(),
                        newIndex,
                        catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ).stream().map( e -> e.unwrap( AllocationTable.class ).orElseThrow() ).collect( Collectors.toList() ) );//catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( ds.getAdapterId(), mergedTable.id ) );
            }
        }

        // Needs to be separated from loop above. Otherwise, we loose data
        sources.forEach( s -> deleteAllocation( statement, s ) );
        property.partitionIds.forEach( id -> catalog.getAllocRel( table.namespaceId ).deletePartition( id ) );
        // Loop over **old.partitionIds** to delete all partitions which are part of table
        // Needs to be done separately because partitionPlacements will be recursively dropped in `deletePartitionGroup` but are needed in dropTable
        for ( long partitionGroupId : property.partitionGroupIds ) {
            catalog.getAllocRel( table.namespaceId ).deletePartitionGroup( partitionGroupId );
        }

        catalog.updateSnapshot();
        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private LogicalColumn addColumn( long namespaceId, String columnName, ColumnTypeInformation typeInformation, Collation collation, PolyValue defaultValue, long tableId, int position ) {
        columnName = adjustNameIfNeeded( columnName, namespaceId );
        LogicalColumn addedColumn = catalog.getLogicalRel( namespaceId ).addColumn(
                columnName,
                tableId,
                position,
                typeInformation.type,
                typeInformation.collectionType,
                typeInformation.precision,
                typeInformation.scale,
                typeInformation.dimension,
                typeInformation.cardinality,
                typeInformation.nullable,
                collation
        );

        // Add default value
        addedColumn = addDefaultValue( namespaceId, defaultValue, addedColumn );

        return addedColumn;
    }


    @Override
    public void createConstraint( ConstraintInformation information, long namespaceId, List<Long> columnIds, long tableId ) {
        String constraintName = information.name;
        if ( constraintName == null ) {
            constraintName = NameGenerator.generateConstraintName();
        }
        switch ( information.getType() ) {
            case UNIQUE:
                catalog.getLogicalRel( namespaceId ).addUniqueConstraint( tableId, constraintName, columnIds );
                break;
            case PRIMARY:
                catalog.getLogicalRel( namespaceId ).addPrimaryKey( tableId, columnIds );
                break;
            case FOREIGN:
                String foreignKeyTable = information.foreignKeyTable;
                long foreignTableId;
                assert foreignKeyTable != null;
                if ( foreignKeyTable.split( "\\." ).length == 1 ) {
                    foreignTableId = catalog.getSnapshot().rel().getTable( namespaceId, foreignKeyTable ).orElseThrow().id;
                } else if ( foreignKeyTable.split( "\\." ).length == 2 ) {
                    foreignTableId = catalog.getSnapshot().rel().getTable( foreignKeyTable.split( "\\." )[0], foreignKeyTable.split( "\\." )[1] ).orElseThrow().id;
                } else {
                    throw new GenericRuntimeException( "Invalid foreign key table name" );
                }
                long columnId = catalog.getSnapshot().rel().getColumn( foreignTableId, information.foreignKeyColumnName ).orElseThrow().id;
                catalog.getLogicalRel( namespaceId ).addForeignKey( tableId, columnIds, foreignTableId, List.of( columnId ), constraintName, ForeignKeyOption.NONE, ForeignKeyOption.NONE );

                break;
        }
    }


    @Override
    public void dropNamespace( String namespaceName, boolean ifExists, Statement statement ) {
        // Check if there is a schema with this name
        Optional<LogicalNamespace> optionalNamespace = catalog.getSnapshot().getNamespace( namespaceName );
        if ( optionalNamespace.isEmpty() ) {
            if ( ifExists ) {
                return;
            }

            throw new GenericRuntimeException( "The namespace does not exist" );
        }

        LogicalNamespace logicalNamespace = optionalNamespace.get();

        // Drop all collections in this namespace
        List<LogicalCollection> collections = catalog.getSnapshot().doc().getCollections( logicalNamespace.id, null );
        for ( LogicalCollection collection : collections ) {
            dropCollection( collection, statement );
        }

        // Drop all tables in this schema
        List<LogicalTable> tables = catalog.getSnapshot().rel().getTables( Pattern.of( namespaceName ), null );
        for ( LogicalTable table : tables ) {
            dropTable( table, statement );
        }

        if ( catalog.getSnapshot().graph().getGraph( logicalNamespace.id ).isPresent() ) {
            dropGraph( logicalNamespace.id, ifExists, statement );
        }

        // Drop schema
        catalog.dropNamespace( logicalNamespace.id );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

    }


    @Override
    public void dropView( LogicalTable view, Statement statement ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        // Make sure that this is a table of type VIEW
        if ( view.entityType != EntityType.VIEW ) {
            throw new GenericRuntimeException( "Can only drop views with this method" );
        }

        // Check if views are dependent from this view
        checkViewDependencies( view );

        catalog.getLogicalRel( view.namespaceId ).flagTableForDeletion( view.id, true );
        // catalog.getLogicalRel( catalogView.namespaceId ).deleteViewDependencies( (LogicalView) catalogView );

        // Delete columns

        for ( LogicalColumn column : snapshot.rel().getColumns( view.id ) ) {
            catalog.getLogicalRel( view.namespaceId ).deleteColumn( column.id );
        }

        // Delete the view
        catalog.getLogicalRel( view.namespaceId ).deleteTable( view.id );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropMaterializedView( LogicalTable materializedView, Statement statement ) {
        // Make sure that this is a table of type Materialized View
        if ( materializedView.entityType != EntityType.MATERIALIZED_VIEW ) {
            throw new GenericRuntimeException( "Only materialized views can be dropped with this method" );
        }
        // Check if views are dependent from this view
        checkViewDependencies( materializedView );

        catalog.getLogicalRel( materializedView.namespaceId ).flagTableForDeletion( materializedView.id, true );
        // catalog.getLogicalRel( materializedView.namespaceId ).deleteViewDependencies( (LogicalView) materializedView );

        dropTable( materializedView, statement );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropTable( LogicalTable table, Statement statement ) {
        // todo Make sure that all adapters are of type storeId (and not source)
        Snapshot snapshot = catalog.getSnapshot();

        // delete all allocations and physicals
        for ( AllocationEntity allocation : snapshot.alloc().getFromLogical( table.id ) ) {
            deleteAllocation( statement, allocation );
        }

        // delete all partitions
        for ( AllocationPartition partition : snapshot.alloc().getPartitionsFromLogical( table.id ) ) {
            catalog.getAllocRel( table.namespaceId ).deletePartition( partition.id );
        }

        // delete all partition groups
        for ( AllocationPartitionGroup group : snapshot.alloc().getPartitionGroupsFromLogical( table.id ) ) {
            catalog.getAllocRel( table.namespaceId ).deletePartitionGroup( group.id );
        }

        // delete all placements
        for ( AllocationPlacement placement : snapshot.alloc().getPlacementsFromLogical( table.id ) ) {
            for ( AllocationColumn column : snapshot.alloc().getColumns( placement.id ) ) {
                catalog.getAllocRel( table.namespaceId ).deleteColumn( placement.id, column.columnId );
            }
            catalog.getAllocRel( table.namespaceId ).deletePlacement( placement.id );
        }

        catalog.getAllocRel( table.namespaceId ).deleteProperty( table.id );

        // delete constraints
        for ( LogicalConstraint constraint : snapshot.rel().getConstraints( table.id ) ) {
            dropConstraint( table, constraint.name );
            //catalog.getLogicalRel( table.namespaceId ).deleteConstraint( constraint.id );
        }

        // delete keys
        for ( LogicalKey key : snapshot.rel().getTableKeys( table.id ) ) {
            catalog.getLogicalRel( table.namespaceId ).deleteKey( key.id );
        }

        // delete indexes
        for ( LogicalIndex index : snapshot.rel().getIndexes( table.id, false ) ) {
            catalog.getLogicalRel( table.namespaceId ).deleteIndex( index.id );
        }

        // delete logical columns
        for ( LogicalColumn column : snapshot.rel().getColumns( table.id ) ) {
            catalog.getLogicalRel( table.namespaceId ).deleteColumn( column.id );
        }

        catalog.getLogicalRel( table.namespaceId ).deleteTable( table.id );

        // Monitor dropTables for statistics
        prepareMonitoring( statement, Kind.DROP_TABLE, table );

        // ON_COMMIT constraint needs no longer to be enforced if entity does no longer exist
        statement.getTransaction().getLogicalTables().remove( table );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

        catalog.updateSnapshot();
    }


    private void deleteAllocation( Statement statement, AllocationEntity allocation ) {
        AdapterManager manager = AdapterManager.getInstance();
        manager.getStore( allocation.adapterId ).orElseThrow().dropTable( statement.getPrepareContext(), allocation.id );

        catalog.getAllocRel( allocation.namespaceId ).deleteAllocation( allocation.id );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void truncate( LogicalTable table, Statement statement ) {
        // Make sure that the table can be modified
        if ( !table.modifiable ) {
            throw new GenericRuntimeException( "Unable to modify a read-only table." );
        }

        // Monitor truncate for rowCount
        prepareMonitoring( statement, Kind.TRUNCATE, table );

        //  Execute truncate on all placements
        List<AllocationEntity> allocations = statement.getTransaction().getSnapshot().alloc().getFromLogical( table.id );
        allocations.forEach( a -> AdapterManager.getInstance().getAdapter( a.adapterId ).orElseThrow().truncate( statement.getPrepareContext(), a.id ) );
    }


    private void prepareMonitoring( Statement statement, Kind kind, LogicalTable catalogTable ) {
        prepareMonitoring( statement, kind, catalogTable, null );
    }


    private void prepareMonitoring( Statement statement, Kind kind, LogicalTable catalogTable, LogicalColumn logicalColumn ) {
        // Initialize Monitoring
        if ( statement.getMonitoringEvent() != null ) {
            return;
        }
        StatementEvent event = new DdlEvent();
        event.setMonitoringType( MonitoringType.from( kind ) );
        event.setTableId( catalogTable.id );
        event.setNamespaceId( catalogTable.namespaceId );
        if ( kind == Kind.DROP_COLUMN ) {
            event.setColumnId( logicalColumn.id );
        }
        statement.setMonitoringEvent( event );

    }


    @Override
    public void dropFunction() {
        throw new GenericRuntimeException( "Not supported yet" );
    }


    @Override
    public void setOption() {
        throw new GenericRuntimeException( "Not supported yet" );
    }


    @Override
    public void createType() {
        throw new GenericRuntimeException( "Not supported yet" );
    }


    @Override
    public void dropType() {
        throw new GenericRuntimeException( "Not supported yet" );
    }

}
