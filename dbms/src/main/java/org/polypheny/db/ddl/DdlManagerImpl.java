/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.AvailableIndexMethod;
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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
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
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.catalog.logistic.NameGenerator;
import org.polypheny.db.catalog.logistic.NamespaceType;
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
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
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


    private LogicalColumn addDefaultValue( long namespaceId, String defaultValue, LogicalColumn column ) {
        if ( defaultValue != null ) {
            // TODO: String is only a temporal solution for default values
            String v = defaultValue;
            if ( v.startsWith( "'" ) ) {
                v = v.substring( 1, v.length() - 1 );
            }
            return catalog.getLogicalRel( namespaceId ).setDefaultValue( column.id, PolyType.VARCHAR, v );
        }
        return column;
    }


    protected DataStore<?> getDataStoreInstance( long storeId ) {
        Adapter<?> adapterInstance = AdapterManager.getInstance().getAdapter( storeId );
        if ( adapterInstance == null ) {
            throw new GenericRuntimeException( "Unknown storeId id: %i", storeId );
        }
        // Make sure it is a data storeId instance
        if ( adapterInstance instanceof DataStore<?> ) {
            return (DataStore<?>) adapterInstance;
        } else if ( adapterInstance instanceof DataSource ) {
            throw new GenericRuntimeException( "Can not use DDLs on Sources" );
        } else {
            throw new GenericRuntimeException( "Unknown kind of adapter: %s", adapterInstance.getClass().getName() );
        }
    }


    @Override
    public long createNamespace( String name, NamespaceType type, boolean ifNotExists, boolean replace ) {
        name = name.toLowerCase();
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
        return catalog.addNamespace( name, type, false );
    }


    @Override
    public void addAdapter( String uniqueName, String adapterName, AdapterType adapterType, Map<String, String> config ) {
        uniqueName = uniqueName.toLowerCase();
        Adapter<?> adapter = AdapterManager.getInstance().addAdapter( adapterName, uniqueName, adapterType, config );
        //catalog.addStoreSnapshot( adapter.storeCatalog );
        if ( adapter instanceof DataSource<?> ) {
            handleSource( (DataSource<?>) adapter );
        }
    }


    private void handleSource( DataSource<?> adapter ) {
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
            if ( catalog.getSnapshot().rel().getTable( Catalog.defaultNamespaceId, tableName ).isPresent() ) { // apparently we put them all into 1?
                int i = 0;
                while ( catalog.getSnapshot().rel().getTable( Catalog.defaultNamespaceId, tableName + i ).isPresent() ) {
                    i++;
                }
                tableName += i;
            }

            LogicalTable logical = catalog.getLogicalRel( Catalog.defaultNamespaceId ).addTable( tableName, EntityType.SOURCE, !(adapter).isDataReadOnly() );
            List<LogicalColumn> columns = new ArrayList<>();
            AllocationPartitionGroup group = catalog.getAllocRel( Catalog.defaultNamespaceId ).addPartitionGroup( logical.id, UNPARTITIONED, Catalog.defaultNamespaceId, PartitionType.NONE, 1, List.of(), false );
            AllocationPartition partition = catalog.getAllocRel( Catalog.defaultNamespaceId ).addPartition( logical.id, Catalog.defaultNamespaceId, group.id, null, false, PlacementType.AUTOMATIC, DataPlacementRole.UP_TO_DATE );

            AllocationPlacement placement = catalog.getAllocRel( Catalog.defaultNamespaceId ).addPlacement( logical.id, Catalog.defaultNamespaceId, adapter.adapterId );
            AllocationEntity allocation = catalog.getAllocRel( Catalog.defaultNamespaceId ).addAllocation( adapter.getAdapterId(), placement.id, partition.id, logical.id );
            List<AllocationColumn> aColumns = new ArrayList<>();
            int colPos = 1;

            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                LogicalColumn column = catalog.getLogicalRel( Catalog.defaultNamespaceId ).addColumn(
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

                AllocationColumn allocationColumn = catalog.getAllocRel( Catalog.defaultNamespaceId ).addColumn(
                        placement.id,
                        logical.id,
                        column.id,
                        adapter.adapterId,
                        PlacementType.STATIC,
                        exportedColumn.physicalPosition ); // Not a valid partitionGroupID --> placeholder

                columns.add( column );
                aColumns.add( allocationColumn );
            }

            catalog.updateSnapshot();
            logical = catalog.getSnapshot().rel().getTable( logical.id ).orElseThrow();
            allocation = catalog.getSnapshot().alloc().getEntity( allocation.id ).orElseThrow();

            buildNamespace( Catalog.defaultNamespaceId, logical, adapter );
            adapter.createTable( null, LogicalTableWrapper.of( logical, columns ), AllocationTableWrapper.of( allocation.unwrap( AllocationTable.class ), aColumns ) );


        }
        catalog.updateSnapshot();

    }


    @Override
    public void dropAdapter( String name, Statement statement ) {
        long defaultNamespaceId = 1;
        name = name.replace( "'", "" );

        CatalogAdapter adapter = catalog.getSnapshot().getAdapter( name ).orElseThrow();
        if ( adapter.type == AdapterType.SOURCE ) {
            for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getEntitiesOnAdapter( adapter.id ).orElse( List.of() ) ) {
                // Make sure that there is only one adapter
                if ( catalog.getSnapshot().alloc().getFromLogical( allocation.logicalId ).size() != 1 ) {
                    throw new GenericRuntimeException( "The data source contains entities with more than one placement. This should not happen!" );
                }

                if ( allocation.unwrap( AllocationCollection.class ) != null ) {
                    dropCollection( catalog.getSnapshot().doc().getCollection( allocation.adapterId ).orElseThrow(), statement );
                } else if ( allocation.unwrap( AllocationTable.class ) != null ) {

                    for ( LogicalForeignKey fk : catalog.getSnapshot().rel().getForeignKeys( allocation.logicalId ) ) {
                        catalog.getLogicalRel( defaultNamespaceId ).deleteForeignKey( fk.id );
                    }

                    LogicalTable table = catalog.getSnapshot().rel().getTable( allocation.logicalId ).orElseThrow();

                    // Make sure that there is only one adapter
                    if ( catalog.getSnapshot().alloc().getDataPlacements( allocation.logicalId ).size() != 1 ) {
                        throw new GenericRuntimeException( "The data source contains tables with more than one placement. This should not happen!" );
                    }

                    // Make sure table is of type source
                    if ( table.entityType != EntityType.SOURCE ) {
                        throw new GenericRuntimeException( "Trying to drop a table located on a data source which is not of table type SOURCE. This should not happen!" );
                    }
                    // Delete column placement in catalog
                    for ( AllocationColumn column : allocation.unwrap( AllocationTable.class ).getColumns() ) {
                        catalog.getAllocRel( defaultNamespaceId ).deleteColumn( allocation.id, column.columnId );
                    }

                    // Remove primary keys
                    catalog.getLogicalRel( defaultNamespaceId ).deletePrimaryKey( table.id );

                    // Delete columns
                    for ( LogicalColumn column : catalog.getSnapshot().rel().getColumns( allocation.logicalId ) ) {
                        catalog.getLogicalRel( defaultNamespaceId ).deleteColumn( column.id );
                    }

                    // Delete the table
                    catalog.getLogicalRel( defaultNamespaceId ).deleteTable( table.id );
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
    public void addColumnToSourceTable( LogicalTable table, String columnPhysicalName, String columnLogicalName, String beforeColumnName, String afterColumnName, String defaultValue, Statement statement ) {

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
        DataSource<?> dataSource = (DataSource<?>) AdapterManager.getInstance().getAdapter( adapterId );

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
        /*for ( AllocationEntity allocation : catalog.getSnapshot().alloc().getFromLogical( table.id ) ) {
            AllocationColumn alloc = catalog.getSnapshot().alloc().getColumn( allocation.adapterId, column.id ).orElseThrow();
            catalog.getAllocRel( allocation.namespaceId ).updatePosition( alloc, position );
        }*/
    }


    @Override
    public void addColumn( String columnName, LogicalTable table, String beforeColumnName, String afterColumnName, ColumnTypeInformation type, boolean nullable, String defaultValue, Statement statement ) {
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
        List<DataStore<?>> stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewColumn( addedColumn );

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
                        .addColumn( statement.getPrepareContext(), entity.id, addedColumn );
            }
        }

        catalog.updateSnapshot();
        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void addForeignKey( LogicalTable table, LogicalTable refTable, List<String> columnNames, List<String> refColumnNames, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );
        checkIfDdlPossible( refTable.entityType );

        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();
            columnIds.add( logicalColumn.id );
        }
        List<Long> referencesIds = new LinkedList<>();
        for ( String columnName : refColumnNames ) {
            LogicalColumn logicalColumn = catalog.getSnapshot().rel().getColumn( refTable.id, columnName ).orElseThrow();
            referencesIds.add( logicalColumn.id );
        }
        catalog.getLogicalRel( table.namespaceId ).addForeignKey( table.id, columnIds, refTable.id, referencesIds, constraintName, onUpdate, onDelete );
    }


    @Override
    public void addIndex( LogicalTable table, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, DataStore<?> location, Statement statement ) throws TransactionException {
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
            throw new GenericRuntimeException( "There exist already an index with the name %s", indexName );
        }

        if ( location == null ) {
            if ( RuntimeConfig.DEFAULT_INDEX_PLACEMENT_STRATEGY.getEnum() == DefaultIndexPlacementStrategy.POLYPHENY ) { // Polystore Index
                addPolyphenyIndex( table, indexMethodName, columnNames, indexName, isUnique, statement );
            } else if ( RuntimeConfig.DEFAULT_INDEX_PLACEMENT_STRATEGY.getEnum() == DefaultIndexPlacementStrategy.ONE_DATA_STORE ) {
                if ( indexMethodName != null ) {
                    throw new GenericRuntimeException( "It is not possible to specify a index method if no location has been specified." );
                }
                // Find a storeId that has all required columns
                for ( CatalogDataPlacement dataPlacement : catalog.getSnapshot().alloc().getDataPlacements( table.id ) ) {
                    boolean hasAllColumns = true;
                    if ( !((DataStore<?>) AdapterManager.getInstance().getAdapter( dataPlacement.adapterId )).getAvailableIndexMethods().isEmpty() ) {
                        for ( long columnId : columnIds ) {
                            if ( catalog.getSnapshot().alloc().getEntity( dataPlacement.adapterId, columnId ).isEmpty() ) {
                                hasAllColumns = false;
                            }
                        }
                        if ( hasAllColumns ) {
                            location = (DataStore<?>) AdapterManager.getInstance().getAdapter( dataPlacement.adapterId );
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
                    if ( ((DataStore<?>) AdapterManager.getInstance().getAdapter( placement.adapterId )).getAvailableIndexMethods().size() > 0 ) {
                        for ( long columnId : columnIds ) {
                            if ( catalog.getSnapshot().alloc().getColumn( placement.id, columnId ).isEmpty() ) {
                                hasAllColumns = false;
                            }
                        }
                        if ( hasAllColumns ) {
                            DataStore<?> loc = (DataStore<?>) AdapterManager.getInstance().getAdapter( placement.adapterId );
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
                    throw new GenericRuntimeException( "Unable to create an index on one of the underlying data stores since there is no data storeId that supports indexes and has all required columns!" );
                }
            }
        } else { // Store Index
            addDataStoreIndex( table, indexMethodName, indexName, isUnique, location, statement, columnIds, type );
        }
    }


    private void addDataStoreIndex( LogicalTable catalogTable, String indexMethodName, String indexName, boolean isUnique, DataStore<?> location, Statement statement, List<Long> columnIds, IndexType type ) {
        // Check if all required columns are present on this storeId
        AllocationTable alloc = catalog.getSnapshot().alloc().getEntity( location.getAdapterId(), catalogTable.id ).map( a -> a.unwrap( AllocationTable.class ) ).orElseThrow();

        if ( !new HashSet<>( alloc.getColumns().stream().map( c -> c.columnId ).collect( Collectors.toList() ) ).containsAll( columnIds ) ) {
            throw new GenericRuntimeException( "Not all required columns for this index are placed on this storeId." );
        }

        String method;
        String methodDisplayName;
        if ( indexMethodName != null && !indexMethodName.equalsIgnoreCase( "default" ) ) {
            AvailableIndexMethod aim = null;
            for ( AvailableIndexMethod availableIndexMethod : location.getAvailableIndexMethods() ) {
                if ( availableIndexMethod.name.equals( indexMethodName ) ) {
                    aim = availableIndexMethod;
                }
            }
            if ( aim == null ) {
                throw new GenericRuntimeException( "The used Index method is not known." );
            }
            method = aim.name;
            methodDisplayName = aim.displayName;
        } else {
            method = location.getDefaultIndexMethod().name;
            methodDisplayName = location.getDefaultIndexMethod().displayName;
        }

        LogicalIndex index = catalog.getLogicalRel( catalogTable.namespaceId ).addIndex(
                catalogTable.id,
                columnIds,
                isUnique,
                method,
                methodDisplayName,
                location.getAdapterId(),
                type,
                indexName );

        String physicalName = location.addIndex(
                statement.getPrepareContext(),
                index, alloc );
        catalog.getLogicalRel( catalogTable.namespaceId ).setIndexPhysicalName( index.id, physicalName );
        //catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( location.getAdapterId(), catalogTable.id );
    }


    public void addPolyphenyIndex( LogicalTable table, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, Statement statement ) throws TransactionException {
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
            AvailableIndexMethod aim = null;
            for ( AvailableIndexMethod availableIndexMethod : IndexManager.getAvailableIndexMethods() ) {
                if ( availableIndexMethod.name.equals( indexMethodName ) ) {
                    aim = availableIndexMethod;
                }
            }
            if ( aim == null ) {
                throw new GenericRuntimeException( "The index method is not known" );
            }
            method = aim.name;
            methodDisplayName = aim.displayName;
        } else {
            method = IndexManager.getDefaultIndexMethod().name;
            methodDisplayName = IndexManager.getDefaultIndexMethod().displayName;
        }

        LogicalIndex index = catalog.getLogicalRel( table.namespaceId ).addIndex(
                table.id,
                columnIds,
                isUnique,
                method,
                methodDisplayName,
                0,
                type,
                indexName );

        IndexManager.getInstance().addIndex( index, statement );
    }


    @Override
    public void addDataPlacement( LogicalTable table, List<LogicalColumn> newColumns, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore<?> dataStore, Statement statement ) {

        // check if allocation already exists
        if ( catalog.getSnapshot().alloc().getPlacement( dataStore.getAdapterId(), table.id ).isPresent() ) {
            throw new GenericRuntimeException( "The placement does already exist" );
        }

        List<LogicalColumn> adjustedColumns = new ArrayList<>( newColumns );

        // Check if placement includes primary key columns
        LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();

        for ( long cId : primaryKey.columnIds ) {
            if ( newColumns.stream().noneMatch( c -> c.id == cId ) ) {
                adjustedColumns.add( catalog.getSnapshot().rel().getColumn( cId ).orElseThrow() );
            }
        }
        adjustedColumns = adjustedColumns.stream().sorted( Comparator.comparingLong( c -> c.position ) ).collect( Collectors.toList() );

        AllocationPlacement placement = catalog.getAllocRel( table.namespaceId ).addPlacement( table.id, table.namespaceId, dataStore.adapterId );
        PartitionProperty property = catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();

        addAllocationsForPlacement( table.namespaceId, statement, table, placement.id, adjustedColumns, property.partitionIds, dataStore );

        Catalog.getInstance().updateSnapshot();

        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), catalog.getSnapshot().getAdapter( dataStore.getAdapterId() ).orElseThrow(), table, adjustedColumns, placement );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

    }


    public void addDataPlacementOld( LogicalTable table, List<Long> columnIds, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore<?> dataStore, Statement statement ) {
        List<LogicalColumn> addedColumns = new LinkedList<>();

        List<Long> tempPartitionGroupList = new ArrayList<>();

        AllocationTable allocation;
        if ( catalog.getSnapshot().alloc().getEntity( dataStore.getAdapterId(), table.id ).isPresent() ) {
            throw new GenericRuntimeException( "The placement does already exist" );
        } else {
            allocation = catalog.getAllocRel( table.namespaceId ).addAllocation( dataStore.getAdapterId(), -1, -1, table.id );
        }

        // Check whether the list is empty (this is a shorthand for a full placement)
        if ( columnIds.isEmpty() ) {
            columnIds = ImmutableList.copyOf( catalog.getSnapshot().rel().getColumns( table.id ) ).stream().map( c -> c.id ).collect( Collectors.toList() );
        }

        // Select partitions to create on this placement
        boolean isDataPlacementPartitioned;
        // Needed to ensure that column placements on the same storeId contain all the same partitions
        // Check if this column placement is the first on the data placement
        // If this returns null this means that this is the first placement and partition list can therefore be specified
        List<Long> currentPartList = catalog.getSnapshot().alloc().getPartitionGroupsOnDataPlacement( dataStore.getAdapterId(), table.id );

        isDataPlacementPartitioned = !currentPartList.isEmpty();

        PartitionProperty property = catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();

        if ( !partitionGroupIds.isEmpty() && partitionGroupNames.isEmpty() ) {

            // Abort if a manual partitionList has been specified even though the data placement has already been partitioned
            if ( isDataPlacementPartitioned ) {
                throw new GenericRuntimeException(
                        "The Data Placement for table: '%s' on storeId: "
                                + "'%s' already contains manually specified partitions: %s. Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead",
                        table.name,
                        dataStore.getUniqueName(),
                        currentPartList );
            }

            log.debug( "Table is partitioned and concrete partitionList has been specified " );
            // First convert specified index to correct partitionGroupId
            for ( int partitionGroupId : partitionGroupIds ) {
                // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                try {
                    tempPartitionGroupList.add( property.partitionGroupIds.get( partitionGroupId ) );
                } catch ( IndexOutOfBoundsException e ) {
                    throw new GenericRuntimeException( "Specified Partition-Index: '%s' is not part of table "
                            + "'%s', has only %s partitions", partitionGroupId, table.name, property.numPartitionGroups );
                }
            }
        } else if ( !partitionGroupNames.isEmpty() && partitionGroupIds.isEmpty() ) {

            if ( isDataPlacementPartitioned ) {
                throw new GenericRuntimeException(
                        "WARNING: The Data Placement for table: '%s' on storeId: "
                                + "'%s' already contains manually specified partitions: %s. Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead",
                        table.name,
                        dataStore.getUniqueName(),
                        currentPartList );
            }

            List<AllocationEntity> entities = catalog.getSnapshot().alloc().getFromLogical( table.id );
            for ( String partitionName : partitionGroupNames ) {
                boolean isPartOfTable = false;
                for ( AllocationEntity entity : entities ) {
                    /*if ( partitionName.equals( catalogPartitionGroup.partitionGroupName.toLowerCase() ) ) {
                        tempPartitionGroupList.add( catalogPartitionGroup.id );
                        isPartOfTable = true;
                        break;
                    }*/
                }
                if ( !isPartOfTable ) {
                    throw new GenericRuntimeException(
                            "Specified Partition-Name: '%s' is not part of table "
                                    + "'%s'. Available partitions: %s", partitionName,
                            table.name,
                            String.join( ",", catalog.getSnapshot().alloc().getPartitionGroupNames( table.id ) ) );

                }
            }
        }
        // Simply Place all partitions on placement since nothing has been specified
        else if ( partitionGroupIds.isEmpty() ) {
            log.debug( "Table is partitioned and concrete partitionList has NOT been specified " );

            if ( isDataPlacementPartitioned ) {
                // If DataPlacement already contains partitions then create new placement with same set of partitions.
                tempPartitionGroupList = currentPartList;
            } else {
                tempPartitionGroupList = property.partitionGroupIds;
            }
        }
        //}

        //all internal partitions placed on this storeId
        List<Long> partitionIds = new ArrayList<>();

        // Gather all partitions relevant to add depending on the specified partitionGroup
        tempPartitionGroupList.forEach( pg -> catalog.getSnapshot().alloc().getPartitions( pg ).forEach( p -> partitionIds.add( p.id ) ) );

        // Create column placements
        for ( long cid : columnIds ) {
            catalog.getAllocRel( table.namespaceId ).addColumn(
                    allocation.partitionId,
                    table.id,
                    cid,
                    -1,
                    PlacementType.MANUAL,
                    0 );
            addedColumns.add( catalog.getSnapshot().rel().getColumn( cid ).orElseThrow() );
        }
        // Check if placement includes primary key columns
        LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
        for ( long cid : primaryKey.columnIds ) {
            if ( !columnIds.contains( cid ) ) {
                catalog.getAllocRel( table.namespaceId ).addColumn(
                        allocation.partitionId,
                        table.id,
                        cid,
                        -1,
                        PlacementType.AUTOMATIC,
                        0 );
                addedColumns.add( catalog.getSnapshot().rel().getColumn( cid ).orElseThrow() );
            }
        }

        // Need to create partitionPlacements first in order to trigger schema creation on PolySchemaBuilder
        for ( long partitionId : partitionIds ) {
            catalog.getAllocRel( table.namespaceId ).addPartition(
                    table.namespaceId, dataStore.getAdapterId(),
                    table.id,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.UP_TO_DATE );
        }

        // Make sure that the stores have created the schema
        Catalog.getInstance().getSnapshot();

        // Create table on storeId
        dataStore.createTable( statement.getPrepareContext(), null, (AllocationTableWrapper) null );
        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), catalog.getSnapshot().getAdapter( dataStore.getAdapterId() ).orElseThrow(), null, null, null );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void addPrimaryKey( LogicalTable table, List<String> columnNames, Statement statement ) {
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
            List<Long> pColumnIds = catalog.getSnapshot().alloc().getColumns( placement.id ).stream().map( c -> c.columnId ).collect( Collectors.toList() );
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
                        AdapterManager.getInstance().getStore( placement.adapterId ).addColumn(
                                statement.getPrepareContext(),
                                entity.id,
                                catalog.getSnapshot().rel().getColumn( columnId ).orElseThrow() );
                    }
                }
            }
        }

    }


    @Override
    public void addUniqueConstraint( LogicalTable table, List<String> columnNames, String constraintName ) {
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
            if ( key.columnIds.contains( column.id ) ) {
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
                        .dropColumn(
                                statement.getPrepareContext(),
                                allocation.id,
                                allocationColumn.columnId );
            }

        }
        catalog.getAllocRel( table.namespaceId ).deleteColumn( allocationColumn.placementId, allocationColumn.columnId );
    }


    private void checkModelLogic( LogicalTable catalogTable ) {
        if ( catalogTable.namespaceType == NamespaceType.DOCUMENT ) {
            throw new GenericRuntimeException( "Modification operation is not allowed by schema type DOCUMENT" );
        }
    }


    private void checkModelLogic( LogicalTable catalogTable, String columnName ) {
        if ( catalogTable.namespaceType == NamespaceType.DOCUMENT
                && (columnName.equals( "_data" ) || columnName.equals( "_id" )) ) {
            throw new GenericRuntimeException( "Modification operation is not allowed by schema type DOCUMENT" );
        }
    }


    @Override
    public void dropConstraint( LogicalTable table, String constraintName ) {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfDdlPossible( table.entityType );

        LogicalConstraint constraint = catalog.getSnapshot().rel().getConstraint( table.id, constraintName ).orElseThrow();
        catalog.getLogicalRel( table.namespaceId ).deleteConstraint( constraint.id );
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

        if ( index.location == 0 ) {
            IndexManager.getInstance().deleteIndex( index );
        } else {
            DataStore<?> storeInstance = AdapterManager.getInstance().getStore( index.location );
            storeInstance.dropIndex( statement.getPrepareContext(), index, catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( index.location, table.id ) );
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
                if ( index.location == 0 ) {
                    // Delete polystore index
                    IndexManager.getInstance().deleteIndex( index );
                } else {
                    // Delete index on storeId
                    AdapterManager.getInstance().getStore( index.location ).dropIndex(
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

        // remove placement itself
        catalog.getAllocRel( table.namespaceId ).deletePlacement( placement.id );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private void dropAllocation( long namespaceId, DataStore<?> store, Statement statement, long allocId ) {
        // Physically delete the data from the storeId
        store.dropTable( statement.getPrepareContext(), allocId );

        // Remove allocations, physical will be removed on next rebuild
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
                AdapterManager.getInstance().getStore( allocationColumn.adapterId ).updateColumnType(
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
    public void setDefaultValue( LogicalTable table, String columnName, String defaultValue, Statement statement ) {
        LogicalColumn logicalColumn = (LogicalColumn) catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

        // Check if model permits operation
        checkModelLogic( table, columnName );

        addDefaultValue( table.namespaceId, defaultValue, logicalColumn );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropDefaultValue( LogicalTable table, String columnName, Statement statement ) {
        LogicalColumn logicalColumn = (LogicalColumn) catalog.getSnapshot().rel().getColumn( table.id, columnName ).orElseThrow();

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
                .collect( Collectors.toList() );

        // all
        List<Long> toRemove = currentColumns
                .stream()
                .filter( c -> !columns.contains( c ) )
                .collect( Collectors.toList() );

        List<Long> toAdd = columns.stream().filter( c -> !currentColumns.contains( c ) ).collect( Collectors.toList() );

        // Check if views are dependent from this
        checkViewDependencies( table );

        // add columns
        for ( long columnId : toAdd ) {
            addColumnPlacement( table, Catalog.snapshot().rel().getColumn( columnId ).orElseThrow(), store, statement );
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
                if ( primaryKey.columnIds.contains( allocationColumn.columnId ) ) {
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
            if ( index.location == store.getAdapterId() && index.key.columnIds.contains( allocationColumn.columnId ) ) {
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
        List<Long> columnIdsToRemove = columnsToBeRemoved.stream().map( c -> c.columnId ).collect( Collectors.toList() );

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

                List<Long> effectiveColumnsOnStore = catalog.getSnapshot().alloc().getColumns( dataPlacement.id ).stream().map( c -> c.columnId ).collect( Collectors.toList() );
                List<Long> effectivePartitionsOnStore = catalog.getSnapshot().alloc().getPartitionsFromLogical( dataPlacement.logicalEntityId ).stream().map( p -> p.id ).collect( Collectors.toList() );//dataPlacement.getAllPartitionIds();

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
        List<Long> newPartitions = new ArrayList<>();

        AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( storeId, table.id ).orElseThrow();
        List<AllocationEntity> currentAllocs = catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id );
        List<Long> currentPartitionsId = currentAllocs.stream().map( a -> a.partitionId ).collect( Collectors.toList() );

        //List<Long> currentPartitionGroupsOnStore = catalog.getSnapshot().alloc().getPartitionGroupsOnDataPlacement( storeId, table.id );

        // Get PartitionGroups that have been removed
        /*for ( long partitionGroupId : currentPartitionGroupsOnStore ) {
            if ( !partitionGroupIds.contains( partitionGroupId ) ) {
                catalog.getSnapshot().alloc().getPartitions( partitionGroupId ).forEach( p -> removedPartitions.add( p.id ) );
            }
        }*/
        List<AllocationEntity> removedPartitions = currentAllocs.stream().filter( a -> !partitionIds.contains( a.partitionId ) ).collect( Collectors.toList() );
        List<Long> addedPartitions = partitionIds.stream().filter( id -> !currentPartitionsId.contains( id ) ).collect( Collectors.toList() );

        /*if ( !validatePlacementsConstraints( table.id, storeInstance.getAdapterId(), new ArrayList<>(), removedPartitions ) ) {
            throw new GenericRuntimeException( "Cannot remove the placement as it is the last" );
        }*/

        // Get PartitionGroups that have been newly added
        /*for ( Long partitionGroupId : partitionIds ) {
            if ( !currentPartitionGroupsOnStore.contains( partitionGroupId ) ) {
                catalog.getSnapshot().alloc().getPartitions( partitionGroupId ).forEach( p -> newPartitions.add( p.id ) );
            }
        }*/

        // Copy the data to the newly added column placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        List<AllocationColumn> allocationColumns = catalog.getSnapshot().alloc().getColumns( placement.id );
        List<LogicalColumn> columns = allocationColumns.stream().map( c -> catalog.getSnapshot().rel().getColumn( c.columnId ).orElseThrow() ).collect( Collectors.toList() );

        if ( addedPartitions.size() > 0 ) {
            // Need to create partitionPlacements first in order to trigger schema creation on PolySchemaBuilder
            for ( long partitionId : addedPartitions ) {
                /*catalog.getAllocRel( table.namespaceId ).add(
                        table.namespaceId, storeId.getAdapterId(),
                        table.id,
                        PlacementType.AUTOMATIC,
                        DataPlacementRole.UP_TO_DATE );*/
                addAllocationTable( table.namespaceId, statement, table, columns, placement.id, partitionId, allocationColumns, store );
            }

            //storeId.createTable( statement.getPrepareContext(), LogicalTableWrapper.of( table, null ), (AllocationTableWrapper) null );

            // Get only columns that are actually on that storeId
            // List<LogicalColumn> necessaryColumns = new LinkedList<>();
            // catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerTable( storeId.getAdapterId(), table.id ).forEach( cp -> necessaryColumns.add( catalog.getSnapshot().rel().getColumn( cp.columnId ).orElseThrow() ) );

            dataMigrator.copyData( statement.getTransaction(), catalog.getSnapshot().getAdapter( storeId ).orElseThrow(), table, columns, placement );

            // Add indexes on this new Partition Placement if there is already an index
            for ( LogicalIndex currentIndex : catalog.getSnapshot().rel().getIndexes( table.id, false ) ) {
                if ( currentIndex.location == storeId ) {
                    store.addIndex( statement.getPrepareContext(), currentIndex, catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ).stream().map( a -> a.unwrap( AllocationTable.class ) ).collect( Collectors.toList() ) );
                }
            }
        }

        if ( !removedPartitions.isEmpty() ) {
            //  Remove indexes
            for ( LogicalIndex currentIndex : catalog.getSnapshot().rel().getIndexes( table.id, false ) ) {
                if ( currentIndex.location == storeId ) {
                    store.dropIndex( null, currentIndex, removedPartitions.stream().map( p -> p.partitionId ).collect( Collectors.toList() ) );
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
    public void addColumnPlacement( LogicalTable table, LogicalColumn logicalColumn, DataStore<?> store, Statement statement ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        // Check whether this placement already exists
        snapshot
                .alloc()
                .getEntity( store.getAdapterId(), table.id )
                .orElseThrow( () -> new GenericRuntimeException( "The requested placement does not exist" ) );

        AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( store.adapterId, table.id ).orElseThrow();

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
            //AllocationEntity allocation = catalog.getSnapshot().alloc().getEntity( placement.id, table.id ).orElseThrow();
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
                dataMigrator.copyData( statement.getTransaction(), catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(), table,
                        ImmutableList.of( logicalColumn ), placement );
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
            if ( index.location == store.getAdapterId() && index.key.columnIds.contains( column.id ) ) {
                throw new GenericRuntimeException( "Cannot remove the column %s, as there is a index %s using it", column, index.name );
            }
        }

        if ( !validatePlacementsConstraints( optionalPlacement.get(), List.of( catalog.getSnapshot().alloc().getColumn( optionalPlacement.get().id, column.id ).orElseThrow() ), List.of() ) ) {
            throw new GenericRuntimeException( "Cannot drop the placement as it is the last" );
        }

        // Check whether the column to drop is a primary key
        LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
        if ( primaryKey.columnIds.contains( column.id ) ) {
            throw new GenericRuntimeException( "Cannot drop primary key" );
        }
        for ( AllocationPartition partition : catalog.getSnapshot().alloc().getPartitionsFromLogical( table.id ) ) {
            AllocationEntity allocation = catalog.getSnapshot().alloc().getAlloc( optionalPlacement.get().id, partition.id ).orElseThrow();
            // Drop Column on storeId
            store.dropColumn( statement.getPrepareContext(), allocation.id, column.id );
            //allocation = catalog.getSnapshot().alloc().getEntity( storeId.getAdapterId(), table.id ).orElseThrow();
        }
        // Drop column placement
        catalog.getAllocRel( table.namespaceId ).deleteColumn( optionalPlacement.get().id, column.id );

        catalog.updateSnapshot();

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void alterTableOwner( LogicalTable table, String newOwnerName ) {
        throw new UnsupportedOperationException();
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

        AlgDataType fieldList = algNode.getRowType();

        List<FieldInformation> columns = getColumnInformation( projectedColumns, fieldList );

        Map<Long, List<Long>> underlyingTables = new HashMap<>();

        findUnderlyingTablesOfView( algNode, underlyingTables, fieldList );

        // add check if underlying table is of model document -> mql, relational -> sql
        underlyingTables.keySet().forEach( tableId -> checkModelLangCompatibility( language, namespaceId ) );

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

        AlgDataType fieldList = algRoot.alg.getRowType();

        Map<Long, List<Long>> underlyingTables = new HashMap<>();
        Map<Long, List<Long>> underlying = findUnderlyingTablesOfView( algRoot.alg, underlyingTables, fieldList );

        Snapshot snapshot = statement.getTransaction().getSnapshot();
        LogicalRelSnapshot relSnapshot = snapshot.rel();

        // add check if underlying table is of model document -> mql, relational -> sql
        underlying.keySet().forEach( tableId -> checkModelLangCompatibility( language, namespaceId ) );

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
            ids.put( field.name, addColumn( namespaceId, field.name, field.typeInformation, field.collation, field.defaultValue, view.id, field.position, stores, placementType ) );
        }
        // Sets previously created primary key
        //catalog.getLogicalRel( namespaceId ).addPrimaryKey( view.id, columnIds );

        AllocationPartitionGroup group = catalog.getAllocRel( namespaceId ).addPartitionGroup( view.id, UNPARTITIONED, namespaceId, PartitionType.NONE, 1, List.of(), false );
        AllocationPartition partition = catalog.getAllocRel( namespaceId ).addPartition( view.id, namespaceId, group.id, null, false, PlacementType.AUTOMATIC, DataPlacementRole.UP_TO_DATE );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocRel( namespaceId ).addPlacement( view.id, namespaceId, store.adapterId );

            addAllocationsForPlacement( namespaceId, statement, view, placement.id, List.copyOf( ids.values() ), List.of( partition.id ), store );

        }
        addBlankPartition( namespaceId, view.id, List.of( group.id ), List.of( partition.id ) );

        catalog.updateSnapshot();

        // Selected data from tables is added into the newly crated materialized view
        MaterializedViewManager materializedManager = MaterializedViewManager.getInstance();
        materializedManager.addData( statement.getTransaction(), stores, algRoot, view );
    }


    private void checkModelLangCompatibility( QueryLanguage language, long namespaceId ) {
        LogicalNamespace namespace = catalog.getSnapshot().getNamespace( namespaceId ).orElseThrow();
        if ( namespace.namespaceType != language.getNamespaceType() ) {
            throw new GenericRuntimeException(
                    "The used language cannot execute schema changing queries on this entity with the data model %s.",
                    namespace.getNamespaceType() );
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
        long graphId = catalog.addNamespace( adjustedName, NamespaceType.GRAPH, caseSensitive );

        // add specialized graph
        LogicalGraph logical = catalog.getLogicalGraph( graphId ).addGraph( graphId, adjustedName, modifiable );

        catalog.updateSnapshot();

        AllocationPartition partition = catalog.getAllocGraph( graphId ).addPartition( logical, "undefined" );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocGraph( graphId ).addPlacement( logical, store.adapterId );
            AllocationGraph alloc = catalog.getAllocGraph( graphId ).addAllocation( logical, placement.id, partition.id, store.getAdapterId() );

            store.createGraph( statement.getPrepareContext(), logical, alloc );
        }

        catalog.updateSnapshot();

        return graphId;
    }


    @Override
    public long addGraphAllocation( long graphId, List<DataStore<?>> stores, Statement statement ) {

        LogicalGraph graph = catalog.getSnapshot().graph().getGraph( graphId ).orElseThrow();
        Snapshot snapshot = statement.getTransaction().getSnapshot();

        List<Long> preExistingPlacements = snapshot
                .alloc()
                .getFromLogical( graphId )
                .stream()
                .filter( p -> !stores.stream().map( Adapter::getAdapterId ).collect( Collectors.toList() ).contains( p.adapterId ) )
                .map( p -> p.adapterId )
                .collect( Collectors.toList() );

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

        return graphId;
    }


    @Override
    public void removeGraphAllocation( long graphId, DataStore<?> store, Statement statement ) {
        AllocationGraph alloc = statement.getTransaction().getSnapshot()
                .alloc()
                .getEntity( store.getAdapterId(), graphId )
                .map( a -> a.unwrap( AllocationGraph.class ) )
                .orElseThrow();

        store.dropGraph( statement.getPrepareContext(), alloc );

        catalog.getAllocGraph( graphId ).deleteAllocation( alloc.id );

        catalog.getAllocGraph( graphId ).removePlacement( alloc.placementId );
    }


    @Override
    public void addGraphAlias( long graphId, String alias, boolean ifNotExists ) {
        catalog.getLogicalGraph( graphId ).addGraphAlias( graphId, alias, ifNotExists );
    }


    @Override
    public void removeGraphAlias( long graphId, String alias, boolean ifNotExists ) {
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
            AdapterManager.getInstance().getStore( alloc.adapterId ).dropGraph( statement.getPrepareContext(), alloc.unwrap( AllocationGraph.class ) );
            catalog.getAllocGraph( alloc.namespaceId ).deleteAllocation( alloc.id );
            catalog.getAllocGraph( alloc.namespaceId ).removePlacement( alloc.placementId );
        }

        catalog.getAllocGraph( graphId ).removePartition( catalog.getSnapshot().alloc().getPartitionsFromLogical( graphId ).get( 0 ).id );
        catalog.getLogicalGraph( graphId ).deleteGraph( graphId );

        catalog.deleteNamespace( graphId );
    }


    private List<FieldInformation> getColumnInformation( List<String> projectedColumns, AlgDataType fieldList ) {
        return getColumnInformation( projectedColumns, fieldList, false, 0 );
    }


    private List<FieldInformation> getColumnInformation( List<String> projectedColumns, AlgDataType fieldList, boolean addPrimary, long tableId ) {
        List<FieldInformation> columns = new ArrayList<>();

        int position = 1;
        for ( AlgDataTypeField alg : fieldList.getFieldList() ) {
            AlgDataType type = alg.getValue();
            if ( alg.getType().getPolyType() == PolyType.ARRAY ) {
                type = alg.getValue().getComponentType();
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
                            alg.getValue().getPolyType() == PolyType.ARRAY ? (int) ((ArrayType) alg.getValue()).getDimension() : -1,
                            alg.getValue().getPolyType() == PolyType.ARRAY ? (int) ((ArrayType) alg.getValue()).getCardinality() : -1,
                            alg.getValue().isNullable() ),
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
        LogicalTable table = algNode.getEntity().unwrap( LogicalTable.class );
        List<LogicalColumn> columns = Catalog.getInstance().getSnapshot().rel().getColumns( table.id );
        List<String> logicalColumnNames = columns.stream().map( c -> c.name ).collect( Collectors.toList() );
        List<Long> underlyingColumns = new ArrayList<>();
        for ( int i = 0; i < columns.size(); i++ ) {
            for ( AlgDataTypeField algDataTypeField : fieldList.getFieldList() ) {
                String name = logicalColumnNames.get( i );
                if ( algDataTypeField.getName().equals( name ) ) {
                    underlyingColumns.add( columns.get( i ).id );
                }
            }
        }
        return underlyingColumns;
    }


    public void createTableOld( long namespaceId, String name, List<FieldInformation> fields, List<ConstraintInformation> constraints, boolean ifNotExists, List<DataStore<?>> stores, PlacementType placementType, Statement statement ) {
        String adjustedName = adjustNameIfNeeded( name, namespaceId );

        // Check if there is already an entity with this name
        if ( assertEntityExists( namespaceId, adjustedName, ifNotExists ) ) {
            return;
        }

        fields = new ArrayList<>( fields );
        constraints = new ArrayList<>( constraints );

        //checkDocumentModel( namespaceId, fields, constraints );

        boolean foundPk = false;
        for ( ConstraintInformation constraintInformation : constraints ) {
            if ( constraintInformation.type == ConstraintType.PRIMARY ) {
                if ( foundPk ) {
                    throw new GenericRuntimeException( "More than one primary key has been provided!" );
                } else {
                    foundPk = true;
                }
            }
        }
        if ( !foundPk ) {
            throw new GenericRuntimeException( "No primary key has been provided!" );
        }

        if ( stores == null ) {
            // Ask router on which storeId(s) the table should be placed
            stores = RoutingManager.getInstance().getCreatePlacementStrategy().getDataStoresForNewEntity();
        }

        LogicalTable table = catalog.getLogicalRel( namespaceId ).addTable(
                name,
                EntityType.ENTITY,
                true );

        // Initially create DataPlacement containers on every storeId the table should be placed.
        // stores.forEach( storeId -> catalog.getAllocRel( namespaceId ).addAllocation( storeId.getAdapterId(), tableId ) );

        // catalog.updateSnapshot();

        for ( FieldInformation information : fields ) {
            addColumn( namespaceId, information.name, information.typeInformation, information.collation, information.defaultValue, table.id, information.position, stores, placementType );
        }

        for ( ConstraintInformation constraint : constraints ) {
            addConstraint( namespaceId, constraint.name, constraint.type, null, table.id );
        }
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        LogicalTable catalogTable = snapshot.rel().getTable( table.id ).orElseThrow();

        // Trigger rebuild of schema; triggers schema creation on adapters
        catalog.updateSnapshot();

        PartitionProperty property = snapshot.alloc().getPartitionProperty( catalogTable.id ).orElseThrow();

        for ( DataStore<?> store : stores ) {
            catalog.getAllocRel( catalogTable.namespaceId ).addPartition(
                    catalogTable.namespaceId,
                    store.getAdapterId(),
                    catalogTable.id,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.UP_TO_DATE );

        }


    }


    @Override
    public void createTable( long namespaceId, String name, List<FieldInformation> fields, List<ConstraintInformation> constraints, boolean ifNotExists, List<DataStore<?>> stores, PlacementType placementType, Statement statement ) {
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
            ids.put( information.name, addColumn( namespaceId, information.name, information.typeInformation, information.collation, information.defaultValue, logical.id, information.position, stores, placementType ) );
        }

        for ( ConstraintInformation constraint : constraints ) {
            addConstraint( namespaceId, constraint.name, constraint.type, constraint.columnNames.stream().map( key -> ids.get( key ).id ).collect( Collectors.toList() ), logical.id );
        }

        // addATable
        AllocationPartition partition = createSinglePartition( namespaceId, logical ).left;

        List<LogicalColumn> columns = ids.values().stream().sorted( Comparator.comparingInt( c -> c.position ) ).collect( Collectors.toList() );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocRel( namespaceId ).addPlacement( logical.id, namespaceId, store.adapterId );

            addAllocationsForPlacement( namespaceId, statement, logical, placement.id, columns, List.of( partition.id ), store );
        }

        catalog.updateSnapshot();
    }


    @NotNull
    private Pair<AllocationPartition, PartitionProperty> createSinglePartition( long namespaceId, LogicalTable logical ) {
        AllocationPartitionGroup group = catalog.getAllocRel( namespaceId ).addPartitionGroup( logical.id, UNPARTITIONED, namespaceId, PartitionType.NONE, 1, List.of(), false );
        AllocationPartition partition = catalog.getAllocRel( namespaceId ).addPartition( logical.id, namespaceId, group.id, null, false, PlacementType.AUTOMATIC, DataPlacementRole.REFRESHABLE );
        PartitionProperty property = addBlankPartition( namespaceId, logical.id, List.of( group.id ), List.of( partition.id ) );
        return Pair.of( partition, property );
    }


    private List<AllocationTable> addAllocationsForPlacement( long namespaceId, Statement statement, LogicalTable logical, long placementId, List<LogicalColumn> lColumns, List<Long> partitionIds, Adapter<?> adapter ) {
        List<AllocationColumn> columns = new ArrayList<>();
        for ( LogicalColumn column : lColumns ) {
            columns.add( catalog.getAllocRel( namespaceId ).addColumn( placementId, logical.id, column.id, adapter.adapterId, PlacementType.AUTOMATIC, column.position ) );
        }

        buildNamespace( namespaceId, logical, adapter );
        List<AllocationTable> tables = new ArrayList<>();
        for ( Long partitionId : partitionIds ) {
            tables.add( addAllocationTable( namespaceId, statement, logical, lColumns, placementId, partitionId, columns, adapter ) );
        }
        return tables;
    }


    private PartitionProperty addBlankPartition( long namespaceId, long logicalEntityId, List<Long> groupIds, List<Long> allocIds ) {
        //LogicalPartitionGroup defaultUnpartitionedGroup = catalog.getAllocRel( namespaceId ).addPartitionGroup( logicalEntityId, "full", namespaceId, PartitionType.NONE, 1, List.of(), true );

        PartitionProperty partitionProperty = PartitionProperty.builder()
                .entityId( logicalEntityId )
                .partitionType( PartitionType.NONE )
                .isPartitioned( false )
                .partitionGroupIds( ImmutableList.copyOf( groupIds ) )
                .partitionIds( ImmutableList.copyOf( allocIds ) )
                .reliesOnPeriodicChecks( false )
                .build();

        catalog.getAllocRel( namespaceId ).addPartitionProperty( logicalEntityId, partitionProperty );
        return partitionProperty;
    }


    private AllocationTable addAllocationTable( long namespaceId, Statement statement, LogicalTable logical, List<LogicalColumn> lColumns, long placementId, long partitionId, List<AllocationColumn> aColumns, Adapter<?> adapter ) {
        AllocationTable alloc = catalog.getAllocRel( namespaceId ).addAllocation( adapter.adapterId, placementId, partitionId, logical.id );

        adapter.createTable( statement.getPrepareContext(), LogicalTableWrapper.of( logical, sortByPosition( lColumns ) ), AllocationTableWrapper.of( alloc, sortByPositionAlloc( aColumns ) ) );
        return alloc;
    }


    @NotNull
    private static List<LogicalColumn> sortByPosition( List<LogicalColumn> columns ) {
        return columns.stream().sorted( Comparator.comparingInt( a -> a.position ) ).collect( Collectors.toList() );
    }


    @NotNull
    private static List<AllocationColumn> sortByPositionAlloc( List<AllocationColumn> columns ) {
        return columns.stream().sorted( Comparator.comparingInt( a -> a.position ) ).collect( Collectors.toList() );
    }


    private void buildNamespace( long namespaceId, LogicalTable logical, Adapter<?> store ) {
        store.updateNamespace( logical.getNamespaceName(), namespaceId );
    }


    @Override
    public void createCollection( long namespaceId, String name, boolean ifNotExists, List<DataStore<?>> stores, PlacementType placementType, Statement statement ) {
        String adjustedName = adjustNameIfNeeded( name, namespaceId );

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

        AllocationPartition partition = catalog.getAllocDoc( namespaceId ).addPartition( logical, "undefined" );

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
        if ( (namespace.namespaceType == NamespaceType.RELATIONAL && snapshot.rel().getTable( namespaceId, name ).isPresent())
                || (namespace.namespaceType == NamespaceType.DOCUMENT && snapshot.doc().getCollection( namespaceId, name ).isPresent())
                || (namespace.namespaceType == NamespaceType.GRAPH && snapshot.graph().getGraph( namespaceId ).isPresent()) ) {
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
            manager.getStore( allocation.adapterId ).dropCollection( statement.getPrepareContext(), allocation.unwrap( AllocationCollection.class ) );

            catalog.getAllocDoc( allocation.namespaceId ).removeAllocation( allocation.id );
            catalog.getAllocDoc( allocation.namespaceId ).removePlacement( allocation.placementId );
        }
        catalog.getAllocDoc( collection.namespaceId ).removePartition( snapshot.alloc().getPartitionsFromLogical( collection.id ).get( 0 ).id );
        catalog.getLogicalDoc( collection.namespaceId ).deleteCollection( collection.id );

        catalog.updateSnapshot();
    }


    @Override
    public void addCollectionAllocation( long namespaceId, String name, List<DataStore<?>> stores, Statement statement ) {
        LogicalCollection collection = catalog.getSnapshot().doc().getCollection( namespaceId, name ).orElseThrow();

        // Initially create DataPlacement containers on every storeId the table should be placed.

        List<Long> preExistingPlacements = catalog.getSnapshot()
                .alloc()
                .getFromLogical( collection.id )
                .stream()
                .filter( p -> !stores.stream().map( Adapter::getAdapterId ).collect( Collectors.toList() ).contains( p.adapterId ) )
                .map( p -> p.adapterId )
                .collect( Collectors.toList() );

        List<AllocationPartition> partitions = catalog.getSnapshot().alloc().getPartitionsFromLogical( collection.id );

        for ( DataStore<?> store : stores ) {
            AllocationPlacement placement = catalog.getAllocDoc( collection.namespaceId ).addPlacement( collection, store.adapterId );
            AllocationCollection alloc = catalog.getAllocDoc( collection.namespaceId ).addAllocation(
                    collection,
                    placement.id,
                    partitions.get( 0 ).id,
                    store.getAdapterId() );

            store.createCollection( statement.getPrepareContext(), collection, alloc );

            if ( !preExistingPlacements.isEmpty() ) {
                // Copy the data to the newly added column placements
                DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
                dataMigrator.copyDocData( alloc, collection, statement.getTransaction() );
            }
        }
    }


    @Override
    public void dropCollectionAllocation( long namespaceId, LogicalCollection collection, List<DataStore<?>> dataStores, Statement statement ) {

        for ( DataStore<?> store : dataStores ) {
            AllocationCollection alloc = catalog.getSnapshot()
                    .alloc()
                    .getEntity( store.adapterId, collection.id ).map( a -> a.unwrap( AllocationCollection.class ) )
                    .orElseThrow();
            store.dropCollection( statement.getPrepareContext(), alloc );

            catalog.getAllocDoc( namespaceId ).removeAllocation( alloc.id );

        }

    }


    /*private void checkDocumentModel( long namespaceId, List<FieldInformation> columns, List<ConstraintInformation> constraints ) {
        if ( catalog.getSnapshot().getNamespace( namespaceId ).namespaceType == NamespaceType.DOCUMENT ) {
            List<String> names = columns.stream().map( c -> c.name ).collect( Collectors.toList() );

            if ( names.contains( "_id" ) ) {
                int index = names.indexOf( "_id" );
                columns.remove( index );
                constraints.remove( index );
                names.remove( "_id" );
            }

            // Add _id column if necessary
            if ( !names.contains( "_id" ) ) {
                ColumnTypeInformation typeInformation = new ColumnTypeInformation( PolyType.VARCHAR, PolyType.VARCHAR, 24, null, null, null, false );
                columns.add( new FieldInformation( "_id", typeInformation, Collation.CASE_INSENSITIVE, null, 0 ) );

            }

            // Remove any primaries
            List<ConstraintInformation> primaries = constraints.stream().filter( c -> c.type == ConstraintType.PRIMARY ).collect( Collectors.toList() );
            if ( primaries.size() > 0 ) {
                primaries.forEach( constraints::remove );
            }

            // Add constraint for _id as primary if necessary
            if ( constraints.stream().noneMatch( c -> c.type == ConstraintType.PRIMARY ) ) {
                constraints.add( new ConstraintInformation( "primary", ConstraintType.PRIMARY, Collections.singletonList( "_id" ) ) );
            }

            if ( names.contains( "_data" ) ) {
                columns.remove( names.indexOf( "_data" ) );
                names.remove( "_data" );
            }

            // Add _data column if necessary
            if ( !names.contains( "_data" ) ) {
                ColumnTypeInformation typeInformation = new ColumnTypeInformation( PolyType.JSON, PolyType.JSON, 1024, null, null, null, false );//new ColumnTypeInformation( PolyType.JSON, PolyType.JSON, 1024, null, null, null, false );
                columns.add( new FieldInformation( "_data", typeInformation, Collation.CASE_INSENSITIVE, null, 1 ) );
            }
        }
    }*/


    @Override
    public void addPartitioning( PartitionInformation partitionInfo, List<DataStore<?>> stores, Statement statement ) throws TransactionException {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        PartitionProperty initialProperty = snapshot.alloc().getPartitionProperty( partitionInfo.table.id ).orElseThrow();
        Pair<List<AllocationPartition>, PartitionProperty> result = addGroupsAndPartitions( partitionInfo, snapshot );

        LogicalTable unPartitionedTable = partitionInfo.table;

        // Update catalog table
        catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionProperty(
                partitionInfo.table.id,
                result.right );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = partitionInfo.table.primaryKey;
        LogicalRelSnapshot relSnapshot = catalog.getSnapshot().rel();
        List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).orElseThrow().columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        LogicalColumn pkColumn = relSnapshot.getColumn( pkColumnIds.get( 0 ) ).orElseThrow();
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
        List<AllocationEntity> sources = new ArrayList<>();
        for ( AllocationPlacement placement : placements ) {
            if ( !fillStores ) {
                continue;
            }
            // Ask router on which storeId(s) the table should be placed
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId );
            if ( !(adapter instanceof DataStore<?>) ) {
                continue;
            }
            DataStore<?> store = (DataStore<?>) adapter;
            stores.add( store );

            AllocationEntity sourceAlloc = catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ).get( 0 );

            List<AllocationColumn> columns = catalog.getSnapshot().alloc().getColumns( placement.id );//allocation.unwrap( AllocationTable.class ).getColumns().stream().map( c -> snapshot.rel().getColumn( c.columnId ).orElseThrow() ).collect( Collectors.toList() );
            List<AllocationTable> partitionAllocations = new ArrayList<>();
            List<LogicalColumn> logicalColumns = columns.stream().map( c -> catalog.getSnapshot().rel().getColumn( c.columnId ).orElseThrow() ).collect( Collectors.toList() );
            for ( AllocationPartition partition : result.left ) {
                /*catalog.getAllocRel( partitionInfo.table.namespaceId ).add(
                        partitionedTable.namespaceId,
                        storeId.getAdapterId(),
                        unPartitionedTable.id,
                        PlacementType.AUTOMATIC,
                        DataPlacementRole.UP_TO_DATE );*/

                partitionAllocations.add( addAllocationTable( partitionInfo.table.namespaceId, statement, unPartitionedTable, logicalColumns, placement.id, partition.id, columns, store ) );
            }
            newAllocations.put( placement, partitionAllocations );

            // First create new tables
            //storeId.createTable( statement.getPrepareContext(), null, (AllocationTableWrapper) null );

            // Copy data from unpartitioned to partitioned
            // Get only columns that are actually on that storeId
            // Every storeId of a newly partitioned table, initially will hold all partitions
            //List<LogicalColumn> necessaryColumns = new ArrayList<>();
            //catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerTable( storeId.getAdapterId(), unPartitionedTable.id ).forEach( cp -> necessaryColumns.add( relSnapshot.getColumn( cp.columnId ).orElseThrow() ) );
            //catalog.updateSnapshot();
            // Copy data from the old partition to new partitions
            catalog.updateSnapshot();
            dataMigrator.copyAllocationData(
                    statement.getTransaction(),
                    catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(),
                    List.of( sourceAlloc.unwrap( AllocationTable.class ) ),
                    result.right,
                    newAllocations.get( placement ),
                    unPartitionedTable );
            sources.add( sourceAlloc );

        }

        // Adjust indexes
        List<LogicalIndex> indexes = relSnapshot.getIndexes( unPartitionedTable.id, false );
        for ( LogicalIndex index : indexes ) {
            // Remove old index
            DataStore<?> ds = ((DataStore<?>) AdapterManager.getInstance().getAdapter( index.location ));
            ds.dropIndex( statement.getPrepareContext(), index, result.right.partitionIds );
            catalog.getLogicalRel( partitionInfo.table.namespaceId ).deleteIndex( index.id );
            // Add new index
            LogicalIndex newIndex = catalog.getLogicalRel( partitionInfo.table.namespaceId ).addIndex(
                    unPartitionedTable.id,
                    index.key.columnIds,
                    index.unique,
                    index.method,
                    index.methodDisplayName,
                    index.location,
                    index.type,
                    index.name );
            if ( index.location == 0 ) {
                IndexManager.getInstance().addIndex( newIndex, statement );
            } else {
                String physicalName = ds.addIndex(
                        statement.getPrepareContext(),
                        index, newAllocations.entrySet().stream().filter( e -> e.getKey().adapterId == ds.adapterId ).findFirst().orElseThrow().getValue() );//catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( ds.getAdapterId(), unPartitionedTable.id ) );
                catalog.getLogicalRel( partitionInfo.table.namespaceId ).setIndexPhysicalName( index.id, physicalName );
            }
        }

        // Remove old tables
        sources.forEach( s -> deleteAllocation( statement, s ) );
        //stores.forEach( storeId -> storeId.deleteTable( statement.getPrepareContext(), -1 ) );
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
                .collect( Collectors.toList() );
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

        // Validate partition setup
        if ( !partitionManager.validatePartitionGroupSetup( partitionInfo.qualifiers, numberOfPartitionGroups, partitionInfo.partitionGroupNames, logicalColumn ) ) {
            throw new GenericRuntimeException( "Partitioning failed for table: %s", partitionInfo.table.name );
        }

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
                        new ArrayList<>(),
                        true );
            } else {
                // If no names have been explicitly defined
                partitionGroupName = "part_" + i;
                if ( !partitionInfo.partitionGroupNames.isEmpty() ) {
                    partitionGroupName = partitionInfo.partitionGroupNames.get( i );
                }

                // Mainly needed for HASH
                if ( partitionInfo.qualifiers.isEmpty() ) {
                    group = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                            partitionInfo.table.id,
                            partitionGroupName,
                            partitionInfo.table.namespaceId,
                            actualPartitionType,
                            numberOfPartitionsPerGroup,
                            new ArrayList<>(),
                            false );
                } else {
                    group = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                            partitionInfo.table.id,
                            partitionGroupName,
                            partitionInfo.table.namespaceId,
                            actualPartitionType,
                            numberOfPartitionsPerGroup,
                            partitionInfo.qualifiers.get( i ),
                            false );
                }
            }
            List<AllocationPartition> partitions = new ArrayList<>();
            partitionGroups.put( group, partitions );
        }

        for ( AllocationPartitionGroup group : partitionGroups.keySet() ) {
            partitionGroups.put( group, List.of( catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition(
                    partitionInfo.table.id,
                    partitionInfo.table.namespaceId,
                    group.id,
                    group.name,
                    group.isUnbound,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.REFRESHABLE ) ) );
        }

        //get All PartitionGroups and then get all partitionIds  for each PG and add them to completeList of partitionIds
        List<AllocationPartition> partitions = partitionGroups.values().stream().flatMap( Collection::stream ).collect( Collectors.toList() );

        PartitionProperty partitionProperty;
        if ( actualPartitionType == PartitionType.TEMPERATURE ) {
            long frequencyInterval = ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInterval();
            switch ( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getIntervalUnit().toString() ) {
                case "days":
                    frequencyInterval = frequencyInterval * 60 * 60 * 24;
                    break;

                case "hours":
                    frequencyInterval = frequencyInterval * 60 * 60;
                    break;

                case "minutes":
                    frequencyInterval = frequencyInterval * 60;
                    break;
            }

            int hotPercentageIn = Integer.parseInt( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageIn().toString() );
            int hotPercentageOut = Integer.parseInt( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageOut().toString() );

            //Initially distribute partitions as intended in a running system
            long numberOfPartitionsInHot = (long) numberOfPartitions * hotPercentageIn / 100;
            if ( numberOfPartitionsInHot == 0 ) {
                numberOfPartitionsInHot = 1;
            }

            long numberOfPartitionsInCold = numberOfPartitions - numberOfPartitionsInHot;

            // -1 because one partition is already created in COLD
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
                        DataPlacementRole.UP_TO_DATE ) );
            }

            // -1 because one partition is already created in COLD
            AllocationPartitionGroup secondGroup = new ArrayList<>( partitionGroups.keySet() ).get( 0 );

            for ( int i = 0; i < numberOfPartitionsInCold - 1; i++ ) {
                partitions.add( catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition(
                        partitionInfo.table.id,
                        partitionInfo.table.namespaceId,
                        secondGroup.id,
                        null,
                        false,
                        PlacementType.AUTOMATIC,
                        DataPlacementRole.UP_TO_DATE ) );
            }

            partitionProperty = TemperaturePartitionProperty.builder()
                    .entityId( logicalColumn.tableId )
                    .partitionType( actualPartitionType )
                    .isPartitioned( true )
                    .internalPartitionFunction( PartitionType.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInternalPartitionFunction().toString().toUpperCase() ) )
                    .partitionColumnId( logicalColumn.id )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroups.keySet().stream().map( g -> g.id ).collect( Collectors.toList() ) ) )
                    .partitionIds( ImmutableList.copyOf( partitions.stream().map( p -> p.id ).collect( Collectors.toList() ) ) )
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
        } else {
            partitionProperty = PartitionProperty.builder()
                    .entityId( logicalColumn.tableId )
                    .partitionType( actualPartitionType )
                    .isPartitioned( true )
                    .partitionColumnId( logicalColumn.id )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroups.keySet().stream().map( g -> g.id ).collect( Collectors.toList() ) ) )
                    .partitionIds( ImmutableList.copyOf( partitions.stream().map( p -> p.id ).collect( Collectors.toList() ) ) )
                    .reliesOnPeriodicChecks( false )
                    .build();
        }
        return Pair.of( partitions, partitionProperty );
    }



    /*public void addPartitioningOld( PartitionInformation partitionInfo, List<DataStore<?>> stores, Statement statement ) throws TransactionException {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        LogicalColumn logicalColumn = snapshot.rel().getColumn( partitionInfo.table.id, partitionInfo.columnName ).orElseThrow();

        PartitionType actualPartitionType = PartitionType.getByName( partitionInfo.typeName );

        // Convert partition names and check whether they are unique
        List<String> sanitizedPartitionGroupNames = partitionInfo.partitionGroupNames
                .stream()
                .map( name -> name.trim().toLowerCase() )
                .collect( Collectors.toList() );
        if ( sanitizedPartitionGroupNames.size() != new HashSet<>( sanitizedPartitionGroupNames ).size() ) {
            throw new GenericRuntimeException( "Name is not unique" );
        }

        // Check if specified partitionColumn is even part of the table
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partition group for table: {} with id {} on column: {}", partitionInfo.table.name, partitionInfo.table.id, logicalColumn.id );
        }

        LogicalTable unPartitionedTable = partitionInfo.table;

        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( actualPartitionType );

        // Check whether partition function supports type of partition column
        if ( !partitionManager.supportsColumnOfType( logicalColumn.type ) ) {
            throw new GenericRuntimeException( "The partition function %s does not support columns of type %s", actualPartitionType, logicalColumn.type );
        }

        int numberOfPartitionGroups = partitionInfo.numberOfPartitionGroups;
        // Calculate how many partitions exist if partitioning is applied.
        long partId;
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

        // Validate partition setup
        if ( !partitionManager.validatePartitionGroupSetup( partitionInfo.qualifiers, numberOfPartitionGroups, partitionInfo.partitionGroupNames, logicalColumn ) ) {
            throw new GenericRuntimeException( "Partitioning failed for table: %s", partitionInfo.table.name );
        }

        // Loop over value to create those partitions with partitionKey to uniquelyIdentify partition
        List<Long> partitionGroupIds = new ArrayList<>();
        for ( int i = 0; i < numberOfPartitionGroups; i++ ) {
            String partitionGroupName;

            // Make last partition unbound partition
            if ( partitionManager.requiresUnboundPartitionGroup() && i == numberOfPartitionGroups - 1 ) {
                partId = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                        partitionInfo.table.id,
                        "Unbound",
                        partitionInfo.table.namespaceId,
                        actualPartitionType,
                        numberOfPartitionsPerGroup,
                        new ArrayList<>(),
                        true );
            } else {
                // If no names have been explicitly defined
                if ( partitionInfo.partitionGroupNames.isEmpty() ) {
                    partitionGroupName = "part_" + i;
                } else {
                    partitionGroupName = partitionInfo.partitionGroupNames.get( i );
                }

                // Mainly needed for HASH
                if ( partitionInfo.qualifiers.isEmpty() ) {
                    partId = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                            partitionInfo.table.id,
                            partitionGroupName,
                            partitionInfo.table.namespaceId,
                            actualPartitionType,
                            numberOfPartitionsPerGroup,
                            new ArrayList<>(),
                            false );
                } else {
                    partId = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartitionGroup(
                            partitionInfo.table.id,
                            partitionGroupName,
                            partitionInfo.table.namespaceId,
                            actualPartitionType,
                            numberOfPartitionsPerGroup,
                            partitionInfo.qualifiers.get( i ),
                            false );
                }
            }
            partitionGroupIds.add( partId );
        }

        List<Long> partitionIds = new ArrayList<>();
        //get All PartitionGroups and then get all partitionIds  for each PG and add them to completeList of partitionIds
        //catalog.getLogicalRel( catalogTable.namespaceId ).getPartitionGroups( partitionInfo.table.id ).forEach( pg -> partitionIds.forEach( p -> partitionIds.add( p ) ) );
        partitionGroupIds.forEach( pg -> catalog.getSnapshot().alloc().getPartitions( pg ).forEach( p -> partitionIds.add( p.id ) ) );

        PartitionProperty partitionProperty;
        if ( actualPartitionType == PartitionType.TEMPERATURE ) {
            long frequencyInterval = ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInterval();
            switch ( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getIntervalUnit().toString() ) {
                case "days":
                    frequencyInterval = frequencyInterval * 60 * 60 * 24;
                    break;

                case "hours":
                    frequencyInterval = frequencyInterval * 60 * 60;
                    break;

                case "minutes":
                    frequencyInterval = frequencyInterval * 60;
                    break;
            }

            int hotPercentageIn = Integer.parseInt( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageIn().toString() );
            int hotPercentageOut = Integer.parseInt( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageOut().toString() );

            //Initially distribute partitions as intended in a running system
            long numberOfPartitionsInHot = (long) numberOfPartitions * hotPercentageIn / 100;
            if ( numberOfPartitionsInHot == 0 ) {
                numberOfPartitionsInHot = 1;
            }

            long numberOfPartitionsInCold = numberOfPartitions - numberOfPartitionsInHot;

            // -1 because one partition is already created in COLD
            List<Long> partitionsForHot = new ArrayList<>();
            catalog.getSnapshot().alloc().getPartitions( partitionGroupIds.get( 0 ) ).forEach( p -> partitionsForHot.add( p.id ) );

            // -1 because one partition is already created in HOT
            for ( int i = 0; i < numberOfPartitionsInHot - 1; i++ ) {
                long tempId;
                tempId = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition( partitionInfo.table.id, partitionInfo.table.namespaceId, partitionGroupIds.get( 0 ), partitionInfo.qualifiers.get( 0 ), false );
                partitionIds.add( tempId );
                partitionsForHot.add( tempId );
            }

            catalog.getAllocRel( partitionInfo.table.namespaceId ).updatePartitionGroup( partitionGroupIds.get( 0 ), partitionsForHot );

            // -1 because one partition is already created in COLD
            List<Long> partitionsForCold = new ArrayList<>();
            catalog.getSnapshot().alloc().getPartitions( partitionGroupIds.get( 1 ) ).forEach( p -> partitionsForCold.add( p.id ) );

            for ( int i = 0; i < numberOfPartitionsInCold - 1; i++ ) {
                long tempId;
                tempId = catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition( partitionInfo.table.id, partitionInfo.table.namespaceId, partitionGroupIds.get( 1 ), partitionInfo.qualifiers.get( 1 ), false );
                partitionIds.add( tempId );
                partitionsForCold.add( tempId );
            }

            catalog.getAllocRel( partitionInfo.table.namespaceId ).updatePartitionGroup( partitionGroupIds.get( 1 ), partitionsForCold );

            partitionProperty = TemperaturePartitionProperty.builder()
                    .partitionType( actualPartitionType )
                    .isPartitioned( true )
                    .internalPartitionFunction( PartitionType.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInternalPartitionFunction().toString().toUpperCase() ) )
                    .partitionColumnId( logicalColumn.id )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                    .partitionIds( ImmutableList.copyOf( partitionIds ) )
                    .partitionCostIndication( PartitionCostIndication.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getAccessPattern().toString().toUpperCase() ) )
                    .frequencyInterval( frequencyInterval )
                    .hotAccessPercentageIn( hotPercentageIn )
                    .hotAccessPercentageOut( hotPercentageOut )
                    .reliesOnPeriodicChecks( true )
                    .hotPartitionGroupId( partitionGroupIds.get( 0 ) )
                    .coldPartitionGroupId( partitionGroupIds.get( 1 ) )
                    .numPartitions( partitionIds.size() )
                    .numPartitionGroups( partitionGroupIds.size() )
                    .build();
        } else {
            partitionProperty = PartitionProperty.builder()
                    .partitionType( actualPartitionType )
                    .isPartitioned( true )
                    .partitionColumnId( logicalColumn.id )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                    .partitionIds( ImmutableList.copyOf( partitionIds ) )
                    .reliesOnPeriodicChecks( false )
                    .build();
        }

        // Update catalog table
        catalog.getAllocRel( partitionInfo.table.namespaceId ).partitionTable( partitionInfo.table.id, partitionProperty );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = partitionInfo.table.primaryKey;
        LogicalRelSnapshot relSnapshot = catalog.getSnapshot().rel();
        List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).orElseThrow().columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        LogicalColumn pkColumn = relSnapshot.getColumn( pkColumnIds.get( 0 ) ).orElseThrow();
        // This gets us only one ccp per storeId (first part of PK)

        boolean fillStores = false;
        if ( stores == null ) {
            stores = new ArrayList<>();
            fillStores = true;
        }
        List<AllocationColumn> allocationColumns = snapshot.alloc().getColumnFromLogical( pkColumn.id ).orElseThrow();
        for ( AllocationColumn ccp : allocationColumns ) {
            if ( fillStores ) {
                // Ask router on which storeId(s) the table should be placed
                Adapter<?> adapter = AdapterManager.getInstance().getAdapter( ccp.adapterId );
                if ( adapter instanceof DataStore<?> ) {
                    stores.add( (DataStore<?>) adapter );
                }
            }
        }

        // Now get the partitioned table, partitionInfo still contains the basic/unpartitioned table.
        LogicalTable partitionedTable = relSnapshot.getTable( partitionInfo.table.id ).orElseThrow();
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        for ( DataStore<?> storeId : stores ) {
            for ( long partitionId : partitionIds ) {
                catalog.getAllocRel( partitionInfo.table.namespaceId ).addPartition(
                        partitionedTable.namespaceId,
                        storeId.getAdapterId(),
                        partitionedTable.id,
                        partitionId,
                        PlacementType.AUTOMATIC,
                        DataPlacementRole.UP_TO_DATE );
            }

            // First create new tables
            storeId.createTable( statement.getPrepareContext(), null, (AllocationTableWrapper) null );

            // Copy data from unpartitioned to partitioned
            // Get only columns that are actually on that storeId
            // Every storeId of a newly partitioned table, initially will hold all partitions
            List<LogicalColumn> necessaryColumns = new LinkedList<>();
            catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerTable( storeId.getAdapterId(), partitionedTable.id ).forEach( cp -> necessaryColumns.add( relSnapshot.getColumn( cp.columnId ).orElseThrow() ) );

            // Copy data from the old partition to new partitions
            dataMigrator.copyAllocationData(
                    statement.getTransaction(),
                    catalog.getSnapshot().getAdapter( storeId.getAdapterId() ),
                    unPartitionedTable,
                    partitionedTable,
                    necessaryColumns,
                    snapshot.alloc().getPartitionProperty( unPartitionedTable.id ).orElseThrow().partitionIds,
                    snapshot.alloc().getPartitionProperty( partitionedTable.id ).orElseThrow().partitionIds );
        }

        // Adjust indexes
        List<LogicalIndex> indexes = relSnapshot.getIndexes( unPartitionedTable.id, false );
        for ( LogicalIndex index : indexes ) {
            // Remove old index
            DataStore<?> ds = ((DataStore<?>) AdapterManager.getInstance().getAdapter( index.location ));
            ds.dropIndex( statement.getPrepareContext(), index, snapshot.alloc().getPartitionProperty( unPartitionedTable.id ).orElseThrow().partitionIds );
            catalog.getLogicalRel( partitionInfo.table.namespaceId ).deleteIndex( index.id );
            // Add new index
            LogicalIndex newIndex = catalog.getLogicalRel( partitionInfo.table.namespaceId ).addIndex(
                    partitionedTable.id,
                    index.key.columnIds,
                    index.unique,
                    index.method,
                    index.methodDisplayName,
                    index.location,
                    index.type,
                    index.name );
            if ( index.location == 0 ) {
                IndexManager.getInstance().addIndex( index, statement );
            } else {
                String physicalName = ds.addIndex(
                        statement.getPrepareContext(),
                        index, (List<AllocationTable>) null );//catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( ds.getAdapterId(), unPartitionedTable.id ) );
                catalog.getLogicalRel( partitionInfo.table.namespaceId ).setIndexPhysicalName( index.id, physicalName );
            }
        }

        // Remove old tables
        stores.forEach( storeId -> storeId.deleteTable( statement.getPrepareContext(), -1 ) );
        catalog.getAllocRel( partitionInfo.table.namespaceId ).deletePartitionGroup( unPartitionedTable.id, unPartitionedTable.namespaceId, snapshot.alloc().getPartitionProperty( unPartitionedTable.id ).orElseThrow().partitionIds.get( 0 ) );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }*/


    @Override
    public void removePartitioning( LogicalTable table, Statement statement ) throws TransactionException {
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
        Map<Long, List<AllocationColumn>> placementDistribution;
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( property.partitionType );
        //placementDistribution = partitionManager.getRelevantPlacements( table, null, List.of() );

        // Update catalog table
        catalog.getAllocRel( table.namespaceId ).deleteProperty( tableId );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = table.primaryKey;
        List<Long> pkColumnIds = relSnapshot.getPrimaryKey( pkid ).orElseThrow().columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        LogicalColumn pkColumn = relSnapshot.getColumn( pkColumnIds.get( 0 ) ).orElseThrow();
        // This gets us only one ccp per storeId (first part of PK)

        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        List<LogicalColumn> logicalColumns = snapshot.rel().getColumns( tableId );

        Pair<AllocationPartition, PartitionProperty> partitionProperty = createSinglePartition( table.namespaceId, table );
        List<AllocationEntity> sources = new ArrayList<>();
        // For merge create only full placements on the used stores. Otherwise, partition constraints might not hold
        for ( AllocationPlacement placement : catalog.getSnapshot().alloc().getPlacementsFromLogical( tableId ) ) {

            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId );
            if ( !(adapter instanceof DataStore<?>) ) {
                continue;
            }
            DataStore<?> store = (DataStore<?>) adapter;

            List<AllocationTable> sourceTables = snapshot.alloc().getAllocsOfPlacement( placement.id ).stream().map( t -> t.unwrap( AllocationTable.class ) ).collect( Collectors.toList() );
            List<AllocationColumn> columns = snapshot.alloc().getColumns( placement.id );

            // First create new tables
            AllocationTable targetTable = addAllocationTable( table.namespaceId, statement, table, logicalColumns, placement.id, partitionProperty.left.id, columns, store );

            catalog.updateSnapshot();
            dataMigrator.copyAllocationData(
                    statement.getTransaction(),
                    catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(),
                    sourceTables,
                    partitionProperty.right,
                    List.of( targetTable ),
                    table );
            sources.addAll( sourceTables );
        }

        // Adjust indexes
        List<LogicalIndex> indexes = relSnapshot.getIndexes( table.id, false );
        for ( LogicalIndex index : indexes ) {
            // Remove old index
            DataStore<?> ds = (DataStore<?>) AdapterManager.getInstance().getAdapter( index.location );
            ds.dropIndex( statement.getPrepareContext(), index, property.partitionIds );
            catalog.getLogicalRel( table.namespaceId ).deleteIndex( index.id );
            // Add new index
            LogicalIndex newIndex = catalog.getLogicalRel( table.namespaceId ).addIndex(
                    table.id,
                    index.key.columnIds,
                    index.unique,
                    index.method,
                    index.methodDisplayName,
                    index.location,
                    index.type,
                    index.name );
            if ( index.location == 0 ) {
                IndexManager.getInstance().addIndex( newIndex, statement );
            } else {
                AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( ds.adapterId, tableId ).orElseThrow();
                ds.addIndex(
                        statement.getPrepareContext(),
                        newIndex,
                        catalog.getSnapshot().alloc().getAllocsOfPlacement( placement.id ).stream().map( e -> e.unwrap( AllocationTable.class ) ).collect( Collectors.toList() ) );//catalog.getSnapshot().alloc().getPartitionsOnDataPlacement( ds.getAdapterId(), mergedTable.id ) );
            }
        }

        // Needs to be separated from loop above. Otherwise, we loose data
        sources.forEach( s -> deleteAllocation( statement, s ) );
        property.partitionIds.forEach( id -> catalog.getAllocRel( table.namespaceId ).deletePartition( id ) );
        // Loop over **old.partitionIds** to delete all partitions which are part of table
        // Needs to be done separately because partitionPlacements will be recursively dropped in `deletePartitionGroup` but are needed in deleteTable
        for ( long partitionGroupId : property.partitionGroupIds ) {
            catalog.getAllocRel( table.namespaceId ).deletePartitionGroup( partitionGroupId );
        }

        catalog.updateSnapshot();
        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    private LogicalColumn addColumn( long namespaceId, String columnName, ColumnTypeInformation typeInformation, Collation collation, String defaultValue, long tableId, int position, List<DataStore<?>> stores, PlacementType placementType ) {
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

        /*for ( DataStore s : stores ) {
            AllocationEntity allocation = catalog.getSnapshot().alloc().getEntity( s.getAdapterId(), tableId );
            catalog.getAllocRel( namespaceId ).addColumn(
                    allocation.id,
                    addedColumnId,
                    placementType,
                    null,
                    null, null, position );
        }*/
        return addedColumn;
    }


    @Override
    public void addConstraint( long namespaceId, String constraintName, ConstraintType constraintType, List<Long> columnIds, long tableId ) {
        if ( constraintType == ConstraintType.PRIMARY ) {
            catalog.getLogicalRel( namespaceId ).addPrimaryKey( tableId, columnIds );
        } else if ( constraintType == ConstraintType.UNIQUE ) {
            if ( constraintName == null ) {
                constraintName = NameGenerator.generateConstraintName();
            }
            catalog.getLogicalRel( namespaceId ).addUniqueConstraint( tableId, constraintName, columnIds );
        }
    }


    @Override
    public void dropNamespace( String namespaceName, boolean ifExists, Statement statement ) {
        namespaceName = namespaceName.toLowerCase();

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
        List<LogicalTable> catalogEntities = catalog.getSnapshot().rel().getTables( Pattern.of( namespaceName ), null );
        for ( LogicalTable catalogTable : catalogEntities ) {
            deleteTable( catalogTable, statement );
        }

        // Drop schema
        catalog.deleteNamespace( logicalNamespace.id );

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

        deleteTable( materializedView, statement );

        // Reset query plan cache, implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    public void dropTableOld( LogicalTable catalogTable, Statement statement ) {
        Snapshot snapshot = catalog.getSnapshot();
        // Make sure that this is a table of type TABLE (and not SOURCE)
        //checkIfDdlPossible( catalogEntity.tableType );

        // Check if views dependent on this table
        checkViewDependencies( catalogTable );

        // Check if there are foreign keys referencing this table
        List<LogicalForeignKey> selfRefsToDelete = new LinkedList<>();
        LogicalRelSnapshot relSnapshot = snapshot.rel();
        List<LogicalForeignKey> exportedKeys = relSnapshot.getExportedKeys( catalogTable.id );
        if ( exportedKeys.size() > 0 ) {
            for ( LogicalForeignKey foreignKey : exportedKeys ) {
                if ( foreignKey.tableId == catalogTable.id ) {
                    // If this is a self-reference, drop it later.
                    selfRefsToDelete.add( foreignKey );
                } else {
                    throw new PolyphenyDbException( "Cannot drop table '" + snapshot.getNamespace( catalogTable.namespaceId ) + "." + catalogTable.name + "' because it is being referenced by '" + exportedKeys.get( 0 ).getSchemaName() + "." + exportedKeys.get( 0 ).getTableName() + "'." );
                }
            }
        }

        // Make sure that all adapters are of type storeId (and not source)
        List<CatalogDataPlacement> placements = snapshot.alloc().getDataPlacements( catalogTable.id );
        for ( CatalogDataPlacement placement : placements ) {
            getDataStoreInstance( placement.adapterId );
        }

        // Delete all indexes
        for ( LogicalIndex index : relSnapshot.getIndexes( catalogTable.id, false ) ) {
            if ( index.location == 0 ) {
                // Delete polystore index
                IndexManager.getInstance().deleteIndex( index );
            } else {
                // Delete index on storeId
                AdapterManager.getInstance().getStore( index.location ).dropIndex(
                        statement.getPrepareContext(),
                        index, snapshot.alloc().getPartitionsOnDataPlacement( index.location, catalogTable.id ) );
            }
            // Delete index in catalog
            catalog.getLogicalRel( catalogTable.namespaceId ).deleteIndex( index.id );
        }

        // Delete data from the stores and remove the column placement
        catalog.getLogicalRel( catalogTable.namespaceId ).flagTableForDeletion( catalogTable.id, true );
        List<AllocationPlacement> p = snapshot.alloc().getPlacementsFromLogical( catalogTable.id );
        List<LogicalColumn> columns;
        for ( AllocationPlacement placement : p ) {
            // Delete table on storeId
            List<Long> partitionIdsOnStore = new ArrayList<>();
            snapshot.alloc().getPartitionPlacementsByTableOnAdapter( placement.adapterId, catalogTable.id ).forEach( pl -> partitionIdsOnStore.add( null ) );

            AdapterManager.getInstance().getStore( placement.adapterId ).dropTable( statement.getPrepareContext(), -1 );
            // Delete column placement in catalog
            columns = snapshot.rel().getColumns( catalogTable.id );
            for ( LogicalColumn column : columns ) {
                Optional<AllocationColumn> allocColumn = catalog.getSnapshot().alloc().getColumn( placement.adapterId, column.id );
                allocColumn.ifPresent( allocationColumn -> catalog.getAllocRel( catalogTable.namespaceId ).deleteColumn( placement.id, column.id ) );
            }
        }

        // Delete the self-referencing foreign keys

        for ( LogicalForeignKey foreignKey : selfRefsToDelete ) {
            catalog.getLogicalRel( catalogTable.namespaceId ).deleteForeignKey( foreignKey.id );
        }

        // Delete indexes of this table
        List<LogicalIndex> indexes = relSnapshot.getIndexes( catalogTable.id, false );
        for ( LogicalIndex index : indexes ) {
            catalog.getLogicalRel( catalogTable.namespaceId ).deleteIndex( index.id );
            IndexManager.getInstance().deleteIndex( index );
        }

        // Delete keys and constraints

        // Remove primary key
        catalog.getLogicalRel( catalogTable.namespaceId ).deletePrimaryKey( catalogTable.id );
        // Delete all foreign keys of the table
        List<LogicalForeignKey> foreignKeys = relSnapshot.getForeignKeys( catalogTable.id );
        for ( LogicalForeignKey foreignKey : foreignKeys ) {
            catalog.getLogicalRel( catalogTable.namespaceId ).deleteForeignKey( foreignKey.id );
        }
        // Delete all constraints of the table
        for ( LogicalConstraint constraint : relSnapshot.getConstraints( catalogTable.id ) ) {
            catalog.getLogicalRel( catalogTable.namespaceId ).deleteConstraint( constraint.id );
        }

        // Delete columns
        columns = snapshot.rel().getColumns( catalogTable.id );
        for ( LogicalColumn column : columns ) {
            catalog.getLogicalRel( catalogTable.namespaceId ).deleteColumn( column.id );
        }

        // Delete the table
        catalog.getLogicalRel( catalogTable.namespaceId ).deleteTable( catalogTable.id );

        // Monitor dropTables for statistics
        prepareMonitoring( statement, Kind.DROP_TABLE, catalogTable );

        // ON_COMMIT constraint needs no longer to be enforced if entity does no longer exist
        statement.getTransaction().getLogicalTables().remove( catalogTable );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void deleteTable( LogicalTable table, Statement statement ) {
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
            catalog.getLogicalRel( table.namespaceId ).deleteConstraint( constraint.id );
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
        // statement.getTransaction().getLogicalTables().remove( table );

        // Reset plan cache implementation cache & routing cache
        statement.getQueryProcessor().resetCaches();

        catalog.updateSnapshot();

    }


    private void deleteAllocation( Statement statement, AllocationEntity allocation ) {
        AdapterManager manager = AdapterManager.getInstance();
        manager.getStore( allocation.adapterId ).dropTable( statement.getPrepareContext(), allocation.id );

        catalog.getAllocRel( allocation.namespaceId ).deleteAllocation( allocation.id );
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
        allocations.forEach( a -> AdapterManager.getInstance().getAdapter( a.adapterId ).truncate( statement.getPrepareContext(), a.id ) );
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
