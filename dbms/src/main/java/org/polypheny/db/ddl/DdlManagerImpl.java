/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.AvailableIndexMethod;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.NameGenerator;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.exception.AlterSourceException;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.IndexExistsException;
import org.polypheny.db.ddl.exception.IndexPreventsRemovalException;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.MissingColumnPlacementException;
import org.polypheny.db.ddl.exception.NotNullAndDefaultValueException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementIsPrimaryException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.ddl.exception.UnknownIndexMethodException;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty.PartitionCostIndication;
import org.polypheny.db.partition.raw.RawTemperaturePartitionInformation;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.LogicalView;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;


@Slf4j
public class DdlManagerImpl extends DdlManager {

    private final Catalog catalog;


    public DdlManagerImpl( Catalog catalog ) {
        this.catalog = catalog;
    }


    private void checkIfTableType( TableType tableType ) throws DdlOnSourceException {
        if ( tableType != TableType.TABLE ) {
            throw new DdlOnSourceException();
        }
    }


    private void checkIfViewType( TableType tableType ) throws DdlOnSourceException {
        if ( tableType != TableType.VIEW ) {
            throw new DdlOnSourceException();
        }
    }


    private void checkViewDependencies( CatalogTable catalogTable ) {
        if ( catalogTable.connectedViews.size() > 0 ) {
            List<String> views = new ArrayList<>();
            for ( Long id : catalogTable.connectedViews ) {
                views.add( catalog.getTable( id ).name );
            }
            throw new PolyphenyDbException( "Cannot alter table because of underlying View " + views.stream().map( String::valueOf ).collect( Collectors.joining( (", ") ) ) );
        }
    }


    private void addDefaultValue( String defaultValue, long addedColumnId ) {
        if ( defaultValue != null ) {
            // TODO: String is only a temporal solution for default values
            String v = defaultValue;
            if ( v.startsWith( "'" ) ) {
                v = v.substring( 1, v.length() - 1 );
            }
            catalog.setDefaultValue( addedColumnId, PolyType.VARCHAR, v );
        }
    }


    protected DataStore getDataStoreInstance( int storeId ) throws DdlOnSourceException {
        Adapter adapterInstance = AdapterManager.getInstance().getAdapter( storeId );
        if ( adapterInstance == null ) {
            throw new RuntimeException( "Unknown store id: " + storeId );
        }
        // Make sure it is a data store instance
        if ( adapterInstance instanceof DataStore ) {
            return (DataStore) adapterInstance;
        } else if ( adapterInstance instanceof DataSource ) {
            throw new DdlOnSourceException();
        } else {
            throw new RuntimeException( "Unknown kind of adapter: " + adapterInstance.getClass().getName() );
        }
    }


    private CatalogColumn getCatalogColumn( long tableId, String columnName ) throws ColumnNotExistsException {
        try {
            return catalog.getColumn( tableId, columnName );
        } catch ( UnknownColumnException e ) {
            throw new ColumnNotExistsException( tableId, columnName );
        }
    }


    @Override
    public void createSchema( String name, long databaseId, SchemaType type, int userId, boolean ifNotExists, boolean replace ) throws SchemaAlreadyExistsException {
        // Check if there is already a schema with this name
        if ( catalog.checkIfExistsSchema( databaseId, name ) ) {
            if ( ifNotExists ) {
                // It is ok that there is already a schema with this name because "IF NOT EXISTS" was specified
                return;
            } else if ( replace ) {
                throw new RuntimeException( "Replacing schema is not yet supported." );
            } else {
                throw new SchemaAlreadyExistsException();
            }
        } else {
            long id = catalog.addSchema( name, databaseId, userId, type );
        }
    }


    @Override
    public void addAdapter( String adapterName, String clazzName, Map<String, String> config ) {
        Adapter adapter = AdapterManager.getInstance().addAdapter( clazzName, adapterName, config );
        if ( adapter instanceof DataSource ) {
            Map<String, List<ExportedColumn>> exportedColumns;
            try {
                exportedColumns = ((DataSource) adapter).getExportedColumns();
            } catch ( Exception e ) {
                AdapterManager.getInstance().removeAdapter( adapter.getAdapterId() );
                throw new RuntimeException( "Could not deploy adapter", e );
            }
            // Create table, columns etc.
            for ( Map.Entry<String, List<ExportedColumn>> entry : exportedColumns.entrySet() ) {
                // Make sure the table name is unique
                String tableName = entry.getKey();
                if ( catalog.checkIfExistsTable( 1, tableName ) ) {
                    int i = 0;
                    while ( catalog.checkIfExistsTable( 1, tableName + i ) ) {
                        i++;
                    }
                    tableName += i;
                }

                long tableId = catalog.addTable( tableName, 1, 1, TableType.SOURCE, !((DataSource) adapter).isDataReadOnly() );
                List<Long> primaryKeyColIds = new ArrayList<>();
                int colPos = 1;
                for ( ExportedColumn exportedColumn : entry.getValue() ) {
                    long columnId = catalog.addColumn(
                            exportedColumn.name,
                            tableId,
                            colPos++,
                            exportedColumn.type,
                            exportedColumn.collectionsType,
                            exportedColumn.length,
                            exportedColumn.scale,
                            exportedColumn.dimension,
                            exportedColumn.cardinality,
                            exportedColumn.nullable,
                            Collation.getDefaultCollation() );
                    catalog.addColumnPlacement(
                            adapter.getAdapterId(),
                            columnId,
                            PlacementType.STATIC,
                            exportedColumn.physicalSchemaName,
                            exportedColumn.physicalTableName,
                            exportedColumn.physicalColumnName,
                            null ); // Not a valid partitionGroupID --> placeholder
                    catalog.updateColumnPlacementPhysicalPosition( adapter.getAdapterId(), columnId, exportedColumn.physicalPosition );
                    if ( exportedColumn.primary ) {
                        primaryKeyColIds.add( columnId );
                    }
                }
                try {
                    catalog.addPrimaryKey( tableId, primaryKeyColIds );
                    CatalogTable catalogTable = catalog.getTable( tableId );
                    catalog.addPartitionPlacement(
                            adapter.getAdapterId(),
                            catalogTable.id,
                            catalogTable.partitionProperty.partitionIds.get( 0 ),
                            PlacementType.AUTOMATIC,
                            null,
                            null );
                } catch ( GenericCatalogException e ) {
                    throw new RuntimeException( "Exception while adding primary key" );
                }
            }
        }
    }


    @Override
    public void dropAdapter( String name, Statement statement ) throws UnknownAdapterException {
        if ( name.startsWith( "'" ) ) {
            name = name.substring( 1 );
        }
        if ( name.endsWith( "'" ) ) {
            name = StringUtils.chop( name );
        }

        CatalogAdapter catalogAdapter = catalog.getAdapter( name );
        if ( catalogAdapter.type == AdapterType.SOURCE ) {
            Set<Long> tablesToDrop = new HashSet<>();
            for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapter( catalogAdapter.id ) ) {
                tablesToDrop.add( ccp.tableId );
            }
            // Remove foreign keys
            for ( Long tableId : tablesToDrop ) {
                for ( CatalogForeignKey fk : catalog.getForeignKeys( tableId ) ) {
                    try {
                        catalog.deleteForeignKey( fk.id );
                    } catch ( GenericCatalogException e ) {
                        throw new PolyphenyDbContextException( "Exception while dropping foreign key", e );
                    }
                }
            }
            // Drop tables
            for ( Long tableId : tablesToDrop ) {
                CatalogTable table = catalog.getTable( tableId );

                // Make sure that there is only one adapter
                if ( table.placementsByAdapter.keySet().size() != 1 ) {
                    throw new RuntimeException( "The data source contains tables with more than one placement. This should not happen!" );
                }

                // Make sure table is of type source
                if ( table.tableType != TableType.SOURCE ) {
                    throw new RuntimeException( "Trying to drop a table located on a data source which is not of table type SOURCE. This should not happen!" );
                }

                // Inform routing
                statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapterPerTable( catalogAdapter.id, table.id ) );
                // Delete column placement in catalog
                for ( Long columnId : table.columnIds ) {
                    if ( catalog.checkIfExistsColumnPlacement( catalogAdapter.id, columnId ) ) {
                        catalog.deleteColumnPlacement( catalogAdapter.id, columnId );
                    }
                }

                // Remove primary keys
                try {
                    catalog.deletePrimaryKey( table.id );
                } catch ( GenericCatalogException e ) {
                    throw new PolyphenyDbContextException( "Exception while dropping primary key", e );
                }

                // Delete columns
                for ( Long columnId : table.columnIds ) {
                    catalog.deleteColumn( columnId );
                }

                // Delete the table
                catalog.deleteTable( table.id );
            }

            // Rest plan cache and implementation cache
            statement.getQueryProcessor().resetCaches();
        }
        AdapterManager.getInstance().removeAdapter( catalogAdapter.id );
    }


    @Override
    public void alterSchemaOwner( String schemaName, String ownerName, long databaseId ) throws UnknownUserException, UnknownSchemaException {
        CatalogSchema catalogSchema = catalog.getSchema( databaseId, schemaName );
        CatalogUser catalogUser = catalog.getUser( ownerName );
        catalog.setSchemaOwner( catalogSchema.id, catalogUser.id );
    }


    @Override
    public void renameSchema( String newName, String oldName, long databaseId ) throws SchemaAlreadyExistsException, UnknownSchemaException {
        if ( catalog.checkIfExistsSchema( databaseId, newName ) ) {
            throw new SchemaAlreadyExistsException();
        }
        CatalogSchema catalogSchema = catalog.getSchema( databaseId, oldName );
        catalog.renameSchema( catalogSchema.id, newName );
    }


    @Override
    public void addColumnToSourceTable( CatalogTable catalogTable, String columnPhysicalName, String columnLogicalName, String beforeColumnName, String afterColumnName, String defaultValue, Statement statement ) throws ColumnAlreadyExistsException, DdlOnSourceException, ColumnNotExistsException {

        if ( catalog.checkIfExistsColumn( catalogTable.id, columnLogicalName ) ) {
            throw new ColumnAlreadyExistsException( columnLogicalName, catalogTable.name );
        }

        CatalogColumn beforeColumn = beforeColumnName == null ? null : getCatalogColumn( catalogTable.id, beforeColumnName );
        CatalogColumn afterColumn = afterColumnName == null ? null : getCatalogColumn( catalogTable.id, afterColumnName );

        // Make sure that the table is of table type SOURCE
        checkIfTableType( catalogTable.tableType );

        // Make sure there is only one adapter
        if ( catalog.getColumnPlacement( catalogTable.columnIds.get( 0 ) ).size() != 1 ) {
            throw new RuntimeException( "The table has an unexpected number of placements!" );
        }

        int adapterId = catalog.getColumnPlacement( catalogTable.columnIds.get( 0 ) ).get( 0 ).adapterId;
        DataSource dataSource = (DataSource) AdapterManager.getInstance().getAdapter( adapterId );

        String physicalTableName = catalog.getPartitionPlacement( adapterId, catalogTable.partitionProperty.partitionIds.get( 0 ) ).physicalTableName;
        List<ExportedColumn> exportedColumns = dataSource.getExportedColumns().get( physicalTableName );

        // Check if physicalColumnName is valid
        ExportedColumn exportedColumn = null;
        for ( ExportedColumn ec : exportedColumns ) {
            if ( ec.physicalColumnName.equalsIgnoreCase( columnPhysicalName ) ) {
                exportedColumn = ec;
            }
        }
        if ( exportedColumn == null ) {
            throw new RuntimeException( "Invalid physical column name '" + columnPhysicalName + "'!" );
        }

        // Make sure this physical column has not already been added to this table
        for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapterPerTable( adapterId, catalogTable.id ) ) {
            if ( ccp.physicalColumnName.equalsIgnoreCase( columnPhysicalName ) ) {
                throw new RuntimeException( "The physical column '" + columnPhysicalName + "' has already been added to this table!" );
            }
        }

        int position = updateAdjacentPositions( catalogTable, beforeColumn, afterColumn );

        long columnId = catalog.addColumn(
                columnLogicalName,
                catalogTable.id,
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
        addDefaultValue( defaultValue, columnId );
        CatalogColumn addedColumn = catalog.getColumn( columnId );

        // Add column placement
        catalog.addColumnPlacement(
                adapterId,
                addedColumn.id,
                PlacementType.STATIC,
                exportedColumn.physicalSchemaName,
                exportedColumn.physicalTableName,
                exportedColumn.physicalColumnName,
                null );//Not a valid partitionID --> placeholder

        // Set column position
        catalog.updateColumnPlacementPhysicalPosition( adapterId, columnId, exportedColumn.physicalPosition );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    private int updateAdjacentPositions( CatalogTable catalogTable, CatalogColumn beforeColumn, CatalogColumn afterColumn ) {
        List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
        int position = columns.size() + 1;
        if ( beforeColumn != null || afterColumn != null ) {
            if ( beforeColumn != null ) {
                position = beforeColumn.position;
            } else {
                position = afterColumn.position + 1;
            }
            // Update position of the other columns
            for ( int i = columns.size(); i >= position; i-- ) {
                catalog.setColumnPosition( columns.get( i - 1 ).id, i + 1 );
            }
        }
        return position;
    }


    @Override
    public void addColumn( String columnName, CatalogTable catalogTable, String beforeColumnName, String afterColumnName, ColumnTypeInformation type, boolean nullable, String defaultValue, Statement statement ) throws NotNullAndDefaultValueException, ColumnAlreadyExistsException, ColumnNotExistsException {
        // Check if the column either allows null values or has a default value defined.
        if ( defaultValue == null && !nullable ) {
            throw new NotNullAndDefaultValueException();
        }

        if ( catalog.checkIfExistsColumn( catalogTable.id, columnName ) ) {
            throw new ColumnAlreadyExistsException( columnName, catalogTable.name );
        }
        //
        CatalogColumn beforeColumn = beforeColumnName == null ? null : getCatalogColumn( catalogTable.id, beforeColumnName );
        CatalogColumn afterColumn = afterColumnName == null ? null : getCatalogColumn( catalogTable.id, afterColumnName );

        int position = updateAdjacentPositions( catalogTable, beforeColumn, afterColumn );

        long columnId = catalog.addColumn(
                columnName,
                catalogTable.id,
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
        addDefaultValue( defaultValue, columnId );
        CatalogColumn addedColumn = catalog.getColumn( columnId );

        // Ask router on which stores this column shall be placed
        List<DataStore> stores = statement.getRouter().addColumn( catalogTable, statement );

        // Add column on underlying data stores and insert default value
        for ( DataStore store : stores ) {
            catalog.addColumnPlacement(
                    store.getAdapterId(),
                    addedColumn.id,
                    PlacementType.AUTOMATIC,
                    null, // Will be set later
                    null, // Will be set later
                    null, // Will be set later
                    null );//Not a valid partitionID --> placeholder
            AdapterManager.getInstance().getStore( store.getAdapterId() ).addColumn( statement.getPrepareContext(), catalogTable, addedColumn );
        }

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void addForeignKey( CatalogTable catalogTable, CatalogTable refTable, List<String> columnNames, List<String> refColumnNames, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws UnknownColumnException, GenericCatalogException {
        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
            columnIds.add( catalogColumn.id );
        }
        List<Long> referencesIds = new LinkedList<>();
        for ( String columnName : refColumnNames ) {
            CatalogColumn catalogColumn = catalog.getColumn( refTable.id, columnName );
            referencesIds.add( catalogColumn.id );
        }
        catalog.addForeignKey( catalogTable.id, columnIds, refTable.id, referencesIds, constraintName, onUpdate, onDelete );
    }


    @Override
    public void addIndex( CatalogTable catalogTable, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, DataStore location, Statement statement ) throws UnknownColumnException, UnknownIndexMethodException, GenericCatalogException, UnknownTableException, UnknownUserException, UnknownSchemaException, UnknownKeyException, UnknownDatabaseException, TransactionException, AlterSourceException, IndexExistsException, MissingColumnPlacementException {
        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
            columnIds.add( catalogColumn.id );
        }

        IndexType type = IndexType.MANUAL;

        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new AlterSourceException();
        }

        // Check if there is already an index with this name for this table
        if ( catalog.checkIfExistsIndex( catalogTable.id, indexName ) ) {
            throw new IndexExistsException();
        }

        if ( location == null ) { // Polystore Index
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
                    throw new UnknownIndexMethodException();
                }
                method = aim.name;
                methodDisplayName = aim.displayName;
            } else {
                method = IndexManager.getDefaultIndexMethod().name;
                methodDisplayName = IndexManager.getDefaultIndexMethod().displayName;
            }

            long indexId = catalog.addIndex(
                    catalogTable.id,
                    columnIds,
                    isUnique,
                    method,
                    methodDisplayName,
                    0,
                    type,
                    indexName );

            IndexManager.getInstance().addIndex( catalog.getIndex( indexId ), statement );
        } else { // Store Index

            // Check if there if all required columns are present on this store
            for ( long columnId : columnIds ) {
                if ( !catalog.checkIfExistsColumnPlacement( location.getAdapterId(), columnId ) ) {
                    throw new MissingColumnPlacementException( catalog.getColumn( columnId ).name );
                }
            }

            String method;
            String methodDisplayName;
            if ( indexMethodName != null ) {
                AvailableIndexMethod aim = null;
                for ( AvailableIndexMethod availableIndexMethod : location.getAvailableIndexMethods() ) {
                    if ( availableIndexMethod.name.equals( indexMethodName ) ) {
                        aim = availableIndexMethod;
                    }
                }
                if ( aim == null ) {
                    throw new UnknownIndexMethodException();
                }
                method = aim.name;
                methodDisplayName = aim.displayName;
            } else {
                method = location.getDefaultIndexMethod().name;
                methodDisplayName = location.getDefaultIndexMethod().displayName;
            }

            long indexId = catalog.addIndex(
                    catalogTable.id,
                    columnIds,
                    isUnique,
                    method,
                    methodDisplayName,
                    location.getAdapterId(),
                    type,
                    indexName );

            location.addIndex( statement.getPrepareContext(), catalog.getIndex( indexId ) );
        }
    }


    @Override
    public void addPlacement( CatalogTable catalogTable, List<Long> columnIds, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore dataStore, Statement statement ) throws PlacementAlreadyExistsException {
        List<CatalogColumn> addedColumns = new LinkedList<>();

        List<Long> tempPartitionGroupList = new ArrayList<>();

        // Check whether this placement already exists
        for ( int storeId : catalogTable.placementsByAdapter.keySet() ) {
            if ( storeId == dataStore.getAdapterId() ) {
                throw new PlacementAlreadyExistsException();
            }
        }
        // Check whether the list is empty (this is a short hand for a full placement)
        if ( columnIds.size() == 0 ) {
            columnIds = ImmutableList.copyOf( catalogTable.columnIds );
        }

        // Select partitions to create on this placement
        // if ( catalogTable.isPartitioned ) {
        boolean isDataPlacementPartitioned = false;
        long tableId = catalogTable.id;
        // Needed to ensure that column placements on the same store contain all the same partitions
        // Check if this column placement is the first on the data placement
        // If this returns null this means that this is the first placement and partition list can therefore be specified
        List<Long> currentPartList = new ArrayList<>();
        currentPartList = catalog.getPartitionGroupsOnDataPlacement( dataStore.getAdapterId(), catalogTable.id );

        isDataPlacementPartitioned = !currentPartList.isEmpty();

        if ( !partitionGroupIds.isEmpty() && partitionGroupNames.isEmpty() ) {

            // Abort if a manual partitionList has been specified even though the data placement has already been partitioned
            if ( isDataPlacementPartitioned ) {
                throw new RuntimeException( "WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                        + dataStore.getAdapterName() + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead" );
            }

            log.debug( "Table is partitioned and concrete partitionList has been specified " );
            // First convert specified index to correct partitionGroupId
            for ( int partitionGroupId : partitionGroupIds ) {
                // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                try {
                    tempPartitionGroupList.add( catalogTable.partitionProperty.partitionGroupIds.get( partitionGroupId ) );
                } catch ( IndexOutOfBoundsException e ) {
                    throw new RuntimeException( "Specified Partition-Index: '" + partitionGroupId + "' is not part of table '"
                            + catalogTable.name + "', has only " + catalogTable.partitionProperty.numPartitionGroups + " partitions" );
                }
            }
        } else if ( !partitionGroupNames.isEmpty() && partitionGroupIds.isEmpty() ) {

            if ( isDataPlacementPartitioned ) {
                throw new RuntimeException( "WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                        + dataStore.getAdapterName() + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead" );
            }

            List<CatalogPartitionGroup> catalogPartitionGroups = catalog.getPartitionGroups( tableId );
            for ( String partitionName : partitionGroupNames ) {
                boolean isPartOfTable = false;
                for ( CatalogPartitionGroup catalogPartitionGroup : catalogPartitionGroups ) {
                    if ( partitionName.equals( catalogPartitionGroup.partitionGroupName.toLowerCase() ) ) {
                        tempPartitionGroupList.add( catalogPartitionGroup.id );
                        isPartOfTable = true;
                        break;
                    }
                }
                if ( !isPartOfTable ) {
                    throw new RuntimeException( "Specified Partition-Name: '" + partitionName + "' is not part of table '"
                            + catalogTable.name + "'. Available partitions: " + String.join( ",", catalog.getPartitionGroupNames( tableId ) ) );

                }
            }
        }
        // Simply Place all partitions on placement since nothing has been specified
        else if ( partitionGroupIds.isEmpty() && partitionGroupNames.isEmpty() ) {
            log.debug( "Table is partitioned and concrete partitionList has NOT been specified " );

            if ( isDataPlacementPartitioned ) {
                // If DataPlacement already contains partitions then create new placement with same set of partitions.
                tempPartitionGroupList = currentPartList;
            } else {
                tempPartitionGroupList = catalogTable.partitionProperty.partitionGroupIds;
            }
        }
        //}

        //all internal partitions placed on this store
        List<Long> partitionIds = new ArrayList<>();
        /*partitionIds = catalog.getPartitionsOnDataPlacement(dataStore.getAdapterId(), catalogTable.id );

        if ( partitionIds.isEmpty() ){
            partitionIds.add( (long) -1 );
            //add default value for non-partitioned otherwise CCP wouldn't be created at all
        }*/

        // Gather all partitions relevant to add depending on the specified partitionGroup
        tempPartitionGroupList.forEach( pg -> catalog.getPartitions( pg ).forEach( p -> partitionIds.add( p.id ) ) );

        // Create column placements
        for ( long cid : columnIds ) {
            catalog.addColumnPlacement(
                    dataStore.getAdapterId(),
                    cid,
                    PlacementType.MANUAL,
                    null,
                    null,
                    null,
                    tempPartitionGroupList );
            addedColumns.add( catalog.getColumn( cid ) );
        }
        // Check if placement includes primary key columns
        CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
        for ( long cid : primaryKey.columnIds ) {
            if ( !columnIds.contains( cid ) ) {
                catalog.addColumnPlacement(
                        dataStore.getAdapterId(),
                        cid,
                        PlacementType.AUTOMATIC,
                        null,
                        null,
                        null,
                        tempPartitionGroupList );
                addedColumns.add( catalog.getColumn( cid ) );
            }
        }

        // Need to create partitionPlacements first in order to trigger schema creation on PolySchemaBuilder
        for ( long partitionId : partitionIds ) {
            catalog.addPartitionPlacement(
                    dataStore.getAdapterId(),
                    catalogTable.id,
                    partitionId,
                    PlacementType.AUTOMATIC,
                    null,
                    null );
        }

        // Create table on store
        dataStore.createTable( statement.getPrepareContext(), catalogTable, catalogTable.partitionProperty.partitionIds );
        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( dataStore.getAdapterId() ), addedColumns, partitionIds );
    }


    @Override
    public void addPrimaryKey( CatalogTable catalogTable, List<String> columnNames, Statement statement ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        try {
            CatalogPrimaryKey oldPk = catalog.getPrimaryKey( catalogTable.primaryKey );

            List<Long> columnIds = new LinkedList<>();
            for ( String columnName : columnNames ) {
                CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }
            catalog.addPrimaryKey( catalogTable.id, columnIds );

            // Add new column placements
            long pkColumnId = oldPk.columnIds.get( 0 ); // It is sufficient to check for one because all get replicated on all stores
            List<CatalogColumnPlacement> oldPkPlacements = catalog.getColumnPlacement( pkColumnId );
            for ( CatalogColumnPlacement ccp : oldPkPlacements ) {
                for ( long columnId : columnIds ) {
                    if ( !catalog.checkIfExistsColumnPlacement( ccp.adapterId, columnId ) ) {
                        catalog.addColumnPlacement(
                                ccp.adapterId,
                                columnId,
                                PlacementType.AUTOMATIC,
                                null, // Will be set later
                                null, // Will be set later
                                null, // Will be set later
                                null );
                        AdapterManager.getInstance().getStore( ccp.adapterId ).addColumn(
                                statement.getPrepareContext(),
                                catalog.getTable( ccp.tableId ),
                                catalog.getColumn( columnId ) );
                    }
                }
            }
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void addUniqueConstraint( CatalogTable catalogTable, List<String> columnNames, String constraintName ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        try {
            List<Long> columnIds = new LinkedList<>();
            for ( String columnName : columnNames ) {
                CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }
            catalog.addUniqueConstraint( catalogTable.id, constraintName, columnIds );
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropColumn( CatalogTable catalogTable, String columnName, Statement statement ) throws ColumnNotExistsException {
        if ( catalogTable.columnIds.size() < 2 ) {
            throw new RuntimeException( "Cannot drop sole column of table " + catalogTable.name );
        }

        //check if views are dependent from this view
        checkViewDependencies( catalogTable );

        CatalogColumn column = getCatalogColumn( catalogTable.id, columnName );

        // Check if column is part of an key
        for ( CatalogKey key : catalog.getTableKeys( catalogTable.id ) ) {
            if ( key.columnIds.contains( column.id ) ) {
                if ( catalog.isPrimaryKey( key.id ) ) {
                    throw new PolyphenyDbException( "Cannot drop column '" + column.name + "' because it is part of the primary key." );
                } else if ( catalog.isIndex( key.id ) ) {
                    throw new PolyphenyDbException( "Cannot drop column '" + column.name + "' because it is part of the index with the name: '" + catalog.getIndexes( key ).get( 0 ).name + "'." );
                } else if ( catalog.isForeignKey( key.id ) ) {
                    throw new PolyphenyDbException( "Cannot drop column '" + column.name + "' because it is part of the foreign key with the name: '" + catalog.getForeignKeys( key ).get( 0 ).name + "'." );
                } else if ( catalog.isConstraint( key.id ) ) {
                    throw new PolyphenyDbException( "Cannot drop column '" + column.name + "' because it is part of the constraint with the name: '" + catalog.getConstraints( key ).get( 0 ).name + "'." );
                }
                throw new PolyphenyDbException( "Ok, strange... Something is going wrong here!" );
            }
        }

        // Delete column from underlying data stores
        for ( CatalogColumnPlacement dp : catalog.getColumnPlacementsByColumn( column.id ) ) {
            if ( catalogTable.tableType == TableType.TABLE ) {
                AdapterManager.getInstance().getStore( dp.adapterId ).dropColumn( statement.getPrepareContext(), dp );
            }
            catalog.deleteColumnPlacement( dp.adapterId, dp.columnId );
        }

        // Delete from catalog
        List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
        catalog.deleteColumn( column.id );
        if ( column.position != columns.size() ) {
            // Update position of the other columns
            for ( int i = column.position; i < columns.size(); i++ ) {
                catalog.setColumnPosition( columns.get( i ).id, i );
            }
        }

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropConstraint( CatalogTable catalogTable, String constraintName ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        try {
            CatalogConstraint constraint = catalog.getConstraint( catalogTable.id, constraintName );
            catalog.deleteConstraint( constraint.id );
        } catch ( GenericCatalogException | UnknownConstraintException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropForeignKey( CatalogTable catalogTable, String foreignKeyName ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        try {
            CatalogForeignKey foreignKey = catalog.getForeignKey( catalogTable.id, foreignKeyName );
            catalog.deleteForeignKey( foreignKey.id );
        } catch ( GenericCatalogException | UnknownForeignKeyException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropIndex( CatalogTable catalogTable, String indexName, Statement statement ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        try {
            CatalogIndex index = catalog.getIndex( catalogTable.id, indexName );

            if ( index.location == 0 ) {
                IndexManager.getInstance().deleteIndex( index );
            } else {
                DataStore storeInstance = AdapterManager.getInstance().getStore( index.location );
                storeInstance.dropIndex( statement.getPrepareContext(), index );
            }

            catalog.deleteIndex( index.id );
        } catch ( UnknownIndexException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropPlacement( CatalogTable catalogTable, DataStore storeInstance, Statement statement ) throws PlacementNotExistsException, LastPlacementException {
        // Check whether this placement exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw new PlacementNotExistsException();
        }

        // Check if there are is another placement for every column on this store
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapterPerTable( storeInstance.getAdapterId(), catalogTable.id ) ) {
            List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacement( placement.columnId );
            if ( existingPlacements.size() < 2 ) {
                throw new LastPlacementException();
            }
        }
        // Drop all indexes on this store
        for ( CatalogIndex index : catalog.getIndexes( catalogTable.id, false ) ) {
            if ( index.location == storeInstance.getAdapterId() ) {
                if ( index.location == 0 ) {
                    // Delete polystore index
                    IndexManager.getInstance().deleteIndex( index );
                } else {
                    // Delete index on store
                    AdapterManager.getInstance().getStore( index.location ).dropIndex( statement.getPrepareContext(), index );
                }
                // Delete index in catalog
                catalog.deleteIndex( index.id );
            }
        }
        // Physically delete the data from the store
        storeInstance.dropTable( statement.getPrepareContext(), catalogTable, catalog.getPartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id ) );
        // Inform routing
        statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapterPerTable( storeInstance.getAdapterId(), catalogTable.id ) );
        // Delete placement in the catalog
        List<CatalogColumnPlacement> placements = catalog.getColumnPlacementsOnAdapterPerTable( storeInstance.getAdapterId(), catalogTable.id );
        for ( CatalogColumnPlacement placement : placements ) {
            catalog.deleteColumnPlacement( storeInstance.getAdapterId(), placement.columnId );
        }

        // Remove All
        catalog.deletePartitionGroupsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id );
    }


    @Override
    public void dropPrimaryKey( CatalogTable catalogTable ) throws DdlOnSourceException {
        try {
            // Make sure that this is a table of type TABLE (and not SOURCE)
            checkIfTableType( catalogTable.tableType );
            catalog.deletePrimaryKey( catalogTable.id );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void setColumnType( CatalogTable catalogTable, String columnName, ColumnTypeInformation type, Statement statement ) throws DdlOnSourceException, ColumnNotExistsException, GenericCatalogException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        catalog.setColumnType(
                catalogColumn.id,
                type.type,
                type.collectionType,
                type.precision,
                type.scale,
                type.dimension,
                type.cardinality );
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacement( catalogColumn.id ) ) {
            AdapterManager.getInstance().getStore( placement.adapterId ).updateColumnType(
                    statement.getPrepareContext(),
                    placement,
                    catalog.getColumn( catalogColumn.id ),
                    catalogColumn.type );
        }

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setColumnNullable( CatalogTable catalogTable, String columnName, boolean nullable, Statement statement ) throws ColumnNotExistsException, DdlOnSourceException, GenericCatalogException {
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        catalog.setNullable( catalogColumn.id, nullable );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setColumnPosition( CatalogTable catalogTable, String columnName, String beforeColumnName, String afterColumnName, Statement statement ) throws ColumnNotExistsException {
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        int targetPosition;
        CatalogColumn refColumn;
        if ( beforeColumnName != null ) {
            refColumn = getCatalogColumn( catalogTable.id, beforeColumnName );
            targetPosition = refColumn.position;
        } else {
            refColumn = getCatalogColumn( catalogTable.id, afterColumnName );
            targetPosition = refColumn.position + 1;
        }
        if ( catalogColumn.id == refColumn.id ) {
            throw new RuntimeException( "Same column!" );
        }
        List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
        if ( targetPosition < catalogColumn.position ) {  // Walk from last column to first column
            for ( int i = columns.size(); i >= 1; i-- ) {
                if ( i < catalogColumn.position && i >= targetPosition ) {
                    catalog.setColumnPosition( columns.get( i - 1 ).id, i + 1 );
                } else if ( i == catalogColumn.position ) {
                    catalog.setColumnPosition( catalogColumn.id, columns.size() + 1 );
                }
                if ( i == targetPosition ) {
                    catalog.setColumnPosition( catalogColumn.id, targetPosition );
                }
            }
        } else if ( targetPosition > catalogColumn.position ) { // Walk from first column to last column
            targetPosition--;
            for ( int i = 1; i <= columns.size(); i++ ) {
                if ( i > catalogColumn.position && i <= targetPosition ) {
                    catalog.setColumnPosition( columns.get( i - 1 ).id, i - 1 );
                } else if ( i == catalogColumn.position ) {
                    catalog.setColumnPosition( catalogColumn.id, columns.size() + 1 );
                }
                if ( i == targetPosition ) {
                    catalog.setColumnPosition( catalogColumn.id, targetPosition );
                }
            }
        }
        // Do nothing

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setColumnCollation( CatalogTable catalogTable, String columnName, Collation collation, Statement statement ) throws ColumnNotExistsException, DdlOnSourceException {
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        catalog.setCollation( catalogColumn.id, collation );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void setDefaultValue( CatalogTable catalogTable, String columnName, String defaultValue, Statement statement ) throws ColumnNotExistsException {
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        addDefaultValue( defaultValue, catalogColumn.id );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropDefaultValue( CatalogTable catalogTable, String columnName, Statement statement ) throws ColumnNotExistsException {
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        catalog.deleteDefaultValue( catalogColumn.id );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void modifyColumnPlacement( CatalogTable catalogTable, List<Long> columnIds, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore storeInstance, Statement statement )
            throws PlacementNotExistsException, IndexPreventsRemovalException, LastPlacementException {
        // Check whether this placement already exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw new PlacementNotExistsException();
        }

        // Check if views are dependent from this view
        checkViewDependencies( catalogTable );

        // Check before physical removal if placement would be correct

        // Which columns to remove
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapterPerTable( storeInstance.getAdapterId(), catalogTable.id ) ) {
            if ( !columnIds.contains( placement.columnId ) ) {
                // Check whether there are any indexes located on the store requiring this column
                for ( CatalogIndex index : catalog.getIndexes( catalogTable.id, false ) ) {
                    if ( index.location == storeInstance.getAdapterId() && index.key.columnIds.contains( placement.columnId ) ) {
                        throw new IndexPreventsRemovalException( index.name, catalog.getColumn( placement.columnId ).name );
                    }
                }
                // Check whether the column is a primary key column
                CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                if ( primaryKey.columnIds.contains( placement.columnId ) ) {
                    // Check if the placement type is manual. If so, change to automatic
                    if ( placement.placementType == PlacementType.MANUAL ) {
                        // Make placement manual
                        catalog.updateColumnPlacementType(
                                storeInstance.getAdapterId(),
                                placement.columnId,
                                PlacementType.AUTOMATIC );
                    }
                } else {
                    // It is not a primary key. Remove the column
                    // Check if there are is another placement for this column
                    List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacement( placement.columnId );
                    if ( existingPlacements.size() < 2 ) {
                        throw new LastPlacementException();
                    }
                    // Drop Column on store
                    storeInstance.dropColumn( statement.getPrepareContext(), catalog.getColumnPlacement( storeInstance.getAdapterId(), placement.columnId ) );
                    // Drop column placement
                    catalog.deleteColumnPlacement( storeInstance.getAdapterId(), placement.columnId );
                }
            }
        }

        List<Long> tempPartitionGroupList = new ArrayList<>();

        // Select partitions to create on this placement
        if ( catalogTable.isPartitioned ) {
            long tableId = catalogTable.id;
            // If index partitions are specified
            if ( !partitionGroupIds.isEmpty() && partitionGroupNames.isEmpty() ) {
                // First convert specified index to correct partitionGroupId
                for ( int partitionGroupId : partitionGroupIds ) {
                    // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionGroupList.add( catalogTable.partitionProperty.partitionGroupIds.get( partitionGroupId ) );
                    } catch ( IndexOutOfBoundsException e ) {
                        throw new RuntimeException( "Specified Partition-Index: '" + partitionGroupId + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalogTable.partitionProperty.partitionGroupIds.size() + " partitions" );
                    }
                }
                catalog.updatePartitionGroupsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id, tempPartitionGroupList );
            }
            // If name partitions are specified
            else if ( !partitionGroupNames.isEmpty() && partitionGroupIds.isEmpty() ) {
                List<CatalogPartitionGroup> catalogPartitionGroups = catalog.getPartitionGroups( tableId );
                for ( String partitionName : partitionGroupNames ) {
                    boolean isPartOfTable = false;
                    for ( CatalogPartitionGroup catalogPartitionGroup : catalogPartitionGroups ) {
                        if ( partitionName.equals( catalogPartitionGroup.partitionGroupName.toLowerCase() ) ) {
                            tempPartitionGroupList.add( catalogPartitionGroup.id );
                            isPartOfTable = true;
                            break;
                        }
                    }
                    if ( !isPartOfTable ) {
                        throw new RuntimeException( "Specified partition name: '" + partitionName + "' is not part of table '"
                                + catalogTable.name + "'. Available partitions: " + String.join( ",", catalog.getPartitionGroupNames( tableId ) ) );
                    }
                }
                catalog.updatePartitionGroupsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id, tempPartitionGroupList );
            } else if ( partitionGroupNames.isEmpty() && partitionGroupIds.isEmpty() ) {
                // If nothing has been explicitly specified keep current placement of partitions.
                // Since it's impossible to have a placement without any partitions anyway
                log.debug( "Table is partitioned and concrete partitionList has NOT been specified " );
                tempPartitionGroupList = catalogTable.partitionProperty.partitionGroupIds;
            }
        } else {
            tempPartitionGroupList.add( catalogTable.partitionProperty.partitionGroupIds.get( 0 ) );
        }

        // All internal partitions placed on this store
        List<Long> partitionIds = new ArrayList<>();

        // Gather all partitions relevant to add depending on the specified partitionGroup
        tempPartitionGroupList.forEach( pg -> catalog.getPartitions( pg ).forEach( p -> partitionIds.add( p.id ) ) );

        // Which columns to add
        List<CatalogColumn> addedColumns = new LinkedList<>();

        for ( long cid : columnIds ) {
            if ( catalog.checkIfExistsColumnPlacement( storeInstance.getAdapterId(), cid ) ) {
                CatalogColumnPlacement placement = catalog.getColumnPlacement( storeInstance.getAdapterId(), cid );
                if ( placement.placementType == PlacementType.AUTOMATIC ) {
                    // Make placement manual
                    catalog.updateColumnPlacementType( storeInstance.getAdapterId(), cid, PlacementType.MANUAL );
                }
            } else {
                // Create column placement
                catalog.addColumnPlacement(
                        storeInstance.getAdapterId(),
                        cid,
                        PlacementType.MANUAL,
                        null,
                        null,
                        null,
                        tempPartitionGroupList );
                // Add column on store
                storeInstance.addColumn( statement.getPrepareContext(), catalogTable, catalog.getColumn( cid ) );
                // Add to list of columns for which we need to copy data
                addedColumns.add( catalog.getColumn( cid ) );
            }
        }

        // Copy the data to the newly added column placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        if ( addedColumns.size() > 0 ) {
            dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( storeInstance.getAdapterId() ), addedColumns, partitionIds );
        }
    }


    @Override
    public void modifyPartitionPlacement( CatalogTable catalogTable, List<Long> partitionGroupIds, DataStore storeInstance, Statement statement ) {
        int storeId = storeInstance.getAdapterId();
        List<Long> newPartitions = new ArrayList<>();
        List<Long> removedPartitions = new ArrayList<>();

        List<Long> currentPartitionGroupsOnStore = catalog.getPartitionGroupsOnDataPlacement( storeId, catalogTable.id );

        // Get PartitionGroups that have been removed
        for ( long partitionGroupId : currentPartitionGroupsOnStore ) {
            if ( !partitionGroupIds.contains( partitionGroupId ) ) {
                catalog.getPartitions( partitionGroupId ).forEach( p -> removedPartitions.add( p.id ) );

            }
        }

        // Get PartitionGroups that have been newly added
        for ( Long partitionGroupId : partitionGroupIds ) {
            if ( !currentPartitionGroupsOnStore.contains( partitionGroupId ) ) {
                catalog.getPartitions( partitionGroupId ).forEach( p -> newPartitions.add( p.id ) );
            }
        }

        // Check for the specified columnId if we still have a ColumnPlacement for every partitionGroup
        // Check for removed partitions if every CCP  still has all partitions somewhere
        for ( long partitionId : removedPartitions ) {
            List<Long> tempIds = new ArrayList<>( catalogTable.columnIds );
            boolean partitionChecked = false;

            for ( CatalogPartitionPlacement cpp : catalog.getPartitionPlacements( partitionId ) ) {
                if ( cpp.adapterId == storeId ) {
                    continue;
                }
                catalog.getColumnPlacementsOnAdapter( cpp.adapterId ).forEach( ccp -> tempIds.remove( ccp.columnId ) );
                if ( tempIds.isEmpty() ) {
                    partitionChecked = true;
                    break;
                }
            }

            if ( partitionChecked == false ) {
                throw new RuntimeException( "Invalid partition distribution" );
            }
        }

        // Update
        catalog.updatePartitionGroupsOnDataPlacement( storeId, catalogTable.id, partitionGroupIds );

        // Copy the data to the newly added column placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        if ( newPartitions.size() > 0 ) {
            // Need to create partitionPlacements first in order to trigger schema creation on PolySchemaBuilder
            for ( long partitionId : newPartitions ) {
                catalog.addPartitionPlacement(
                        storeInstance.getAdapterId(),
                        catalogTable.id,
                        partitionId,
                        PlacementType.AUTOMATIC,
                        null,
                        null );
            }

            storeInstance.createTable( statement.getPrepareContext(), catalogTable, newPartitions );

            // Get only columns that are actually on that store
            List<CatalogColumn> necessaryColumns = new LinkedList<>();
            catalog.getColumnPlacementsOnAdapterPerTable( storeInstance.getAdapterId(), catalogTable.id ).forEach( cp -> necessaryColumns.add( catalog.getColumn( cp.columnId ) ) );
            dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( storeId ), necessaryColumns, newPartitions );
        }

        if ( removedPartitions.size() > 0 ) {
            storeInstance.dropTable( statement.getPrepareContext(), catalogTable, removedPartitions );
        }
    }


    @Override
    public void addColumnPlacement( CatalogTable catalogTable, String columnName, DataStore storeInstance, Statement statement ) throws UnknownAdapterException, PlacementNotExistsException, PlacementAlreadyExistsException, ColumnNotExistsException {
        if ( storeInstance == null ) {
            throw new UnknownAdapterException( "" );
        }
        // Check whether this placement already exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw new PlacementNotExistsException();
        }

        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        // Make sure that this store does not contain a placement of this column
        if ( catalog.checkIfExistsColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id ) ) {
            CatalogColumnPlacement placement = catalog.getColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id );
            if ( placement.placementType == PlacementType.AUTOMATIC ) {
                // Make placement manual
                catalog.updateColumnPlacementType(
                        storeInstance.getAdapterId(),
                        catalogColumn.id,
                        PlacementType.MANUAL );
            } else {
                throw new PlacementAlreadyExistsException();
            }
        } else {
            // Create column placement
            catalog.addColumnPlacement(
                    storeInstance.getAdapterId(),
                    catalogColumn.id,
                    PlacementType.MANUAL,
                    null,
                    null,
                    null,
                    null );
            // Add column on store
            storeInstance.addColumn( statement.getPrepareContext(), catalogTable, catalogColumn );
            // Copy the data to the newly added column placements
            DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
            dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( storeInstance.getAdapterId() ),
                    ImmutableList.of( catalogColumn ), catalog.getPartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id ) );
        }
    }


    @Override
    public void dropColumnPlacement( CatalogTable catalogTable, String columnName, DataStore storeInstance, Statement statement ) throws UnknownAdapterException, PlacementNotExistsException, IndexPreventsRemovalException, LastPlacementException, PlacementIsPrimaryException, ColumnNotExistsException {
        if ( storeInstance == null ) {
            throw new UnknownAdapterException( "" );
        }
        // Check whether this placement already exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw new PlacementNotExistsException();
        }

        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        // Check whether this store actually contains a placement of this column
        if ( !catalog.checkIfExistsColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id ) ) {
            throw new PlacementNotExistsException();
        }
        // Check whether there are any indexes located on the store requiring this column
        for ( CatalogIndex index : catalog.getIndexes( catalogTable.id, false ) ) {
            if ( index.location == storeInstance.getAdapterId() && index.key.columnIds.contains( catalogColumn.id ) ) {
                throw new IndexPreventsRemovalException( index.name, columnName );
            }
        }
        // Check if there are is another placement for this column
        List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacement( catalogColumn.id );
        if ( existingPlacements.size() < 2 ) {
            throw new LastPlacementException();
        }
        // Check whether the column to drop is a primary key
        CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
        if ( primaryKey.columnIds.contains( catalogColumn.id ) ) {
            throw new PlacementIsPrimaryException();
        }
        // Drop Column on store
        storeInstance.dropColumn( statement.getPrepareContext(), catalog.getColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id ) );
        // Drop column placement
        catalog.deleteColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id );
    }


    @Override
    public void alterTableOwner( CatalogTable catalogTable, String newOwnerName ) throws UnknownUserException {
        CatalogUser catalogUser = catalog.getUser( newOwnerName );
        catalog.setTableOwner( catalogTable.id, catalogUser.id );
    }


    @Override
    public void renameTable( CatalogTable catalogTable, String newTableName, Statement statement ) throws TableAlreadyExistsException {
        if ( catalog.checkIfExistsTable( catalogTable.schemaId, newTableName ) ) {
            throw new TableAlreadyExistsException();
        }
        //check if views are dependent from this view
        checkViewDependencies( catalogTable );

        catalog.renameTable( catalogTable.id, newTableName );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void renameColumn( CatalogTable catalogTable, String columnName, String newColumnName, Statement statement ) throws ColumnAlreadyExistsException, ColumnNotExistsException {
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );

        if ( catalog.checkIfExistsColumn( catalogColumn.tableId, newColumnName ) ) {
            throw new ColumnAlreadyExistsException( newColumnName, catalogColumn.getTableName() );
        }
        //check if views are dependent from this view
        checkViewDependencies( catalogTable );

        catalog.renameColumn( catalogColumn.id, newColumnName );

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void createView( String viewName, long schemaId, RelNode relNode, RelCollation relCollation, boolean replace, Statement statement, List<DataStore> stores, PlacementType placementType, List<String> projectedColumns ) throws TableAlreadyExistsException {
        if ( catalog.checkIfExistsTable( schemaId, viewName ) ) {
            if ( replace ) {
                try {
                    dropView( catalog.getTable( schemaId, viewName ), statement );
                } catch ( UnknownTableException | DdlOnSourceException e ) {
                    throw new RuntimeException( "Unable tp drop the existing View with this name." );
                }
            } else {
                throw new TableAlreadyExistsException();
            }
        }

        if ( stores == null ) {
            // Ask router on which store(s) the table should be placed
            stores = statement.getRouter().createTable( schemaId, statement );
        }

        prepareView( relNode );
        RelDataType fieldList = relNode.getRowType();

        List<ColumnInformation> columns = new ArrayList<>();

        int position = 1;
        for ( RelDataTypeField rel : fieldList.getFieldList() ) {
            RelDataType type = rel.getValue();
            if ( rel.getType().getPolyType() == PolyType.ARRAY ) {
                type = ((ArrayType) rel.getValue()).getComponentType();
            }
            String colName = rel.getName();
            if ( projectedColumns != null ) {
                colName = projectedColumns.get( position - 1 );
            }

            // type.getPrecision() == RelDataTypeSystemImpl.DEFAULT.getDefaultPrecision(type.getPolyType()) ? type.getPrecision() : -1,
            columns.add( new ColumnInformation(
                    colName.toLowerCase().replaceAll( "[^A-Za-z0-9]", "_" ),
                    new ColumnTypeInformation(
                            type.getPolyType(),
                            rel.getType().getPolyType(),
                            type.getRawPrecision(),
                            type.getScale(),
                            rel.getValue().getPolyType() == PolyType.ARRAY ? (int) ((ArrayType) rel.getValue()).getDimension() : -1,
                            rel.getValue().getPolyType() == PolyType.ARRAY ? (int) ((ArrayType) rel.getValue()).getCardinality() : -1,
                            rel.getValue().isNullable() ),
                    Collation.getDefaultCollation(),
                    null,
                    position ) );
            position++;
        }

        Map<Long, List<Long>> underlyingTables = new HashMap<>();
        long tableId = catalog.addView(
                viewName,
                schemaId,
                statement.getPrepareContext().getCurrentUserId(),
                TableType.VIEW,
                false,
                relNode,
                relCollation,
                findUnderlyingTablesOfView( relNode, underlyingTables, fieldList ),
                fieldList
        );

        for ( ColumnInformation column : columns ) {
            //addColumn( column.name, column.typeInformation, column.collation, column.defaultValue, tableId, column.position, stores, placementType );
            long columnId = catalog.addColumn(
                    column.name,
                    tableId,
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
    }


    private void prepareView( RelNode viewNode ) {
        if ( viewNode instanceof AbstractRelNode ) {
            ((AbstractRelNode) viewNode).setCluster( null );
        }
        if ( viewNode instanceof BiRel ) {
            prepareView( ((BiRel) viewNode).getLeft() );
            prepareView( ((BiRel) viewNode).getRight() );
        } else if ( viewNode instanceof SingleRel ) {
            prepareView( ((SingleRel) viewNode).getInput() );
        }
    }


    private Map<Long, List<Long>> findUnderlyingTablesOfView( RelNode relNode, Map<Long, List<Long>> underlyingTables, RelDataType fieldList ) {
        if ( relNode instanceof LogicalTableScan ) {
            List<Long> underlyingColumns = getUnderlyingColumns( relNode, fieldList );
            underlyingTables.put( ((LogicalTable) ((RelOptTableImpl) relNode.getTable()).getTable()).getTableId(), underlyingColumns );
        } else if ( relNode instanceof LogicalViewTableScan ) {
            List<Long> underlyingColumns = getUnderlyingColumns( relNode, fieldList );
            underlyingTables.put( ((LogicalView) ((RelOptTableImpl) relNode.getTable()).getTable()).getTableId(), underlyingColumns );
        }
        if ( relNode instanceof BiRel ) {
            findUnderlyingTablesOfView( ((BiRel) relNode).getLeft(), underlyingTables, fieldList );
            findUnderlyingTablesOfView( ((BiRel) relNode).getRight(), underlyingTables, fieldList );
        } else if ( relNode instanceof SingleRel ) {
            findUnderlyingTablesOfView( ((SingleRel) relNode).getInput(), underlyingTables, fieldList );
        }
        return underlyingTables;
    }


    private List<Long> getUnderlyingColumns( RelNode relNode, RelDataType fieldList ) {
        List<Long> columnIds = ((LogicalTable) ((RelOptTableImpl) relNode.getTable()).getTable()).getColumnIds();
        List<String> logicalColumnNames = ((LogicalTable) ((RelOptTableImpl) relNode.getTable()).getTable()).getLogicalColumnNames();
        List<Long> underlyingColumns = new ArrayList<>();
        for ( int i = 0; i < columnIds.size(); i++ ) {
            for ( RelDataTypeField relDataTypeField : fieldList.getFieldList() ) {
                String name = logicalColumnNames.get( i );
                if ( relDataTypeField.getName().equals( name ) ) {
                    underlyingColumns.add( columnIds.get( i ) );
                }
            }
        }
        return underlyingColumns;
    }


    @Override
    public void createTable( long schemaId, String tableName, List<ColumnInformation> columns, List<ConstraintInformation> constraints, boolean ifNotExists, List<DataStore> stores, PlacementType placementType, Statement statement ) throws TableAlreadyExistsException, ColumnNotExistsException, UnknownPartitionTypeException, UnknownColumnException, PartitionGroupNamesNotUniqueException {
        try {
            // Check if there is already a table with this name
            if ( catalog.checkIfExistsTable( schemaId, tableName ) ) {
                if ( ifNotExists ) {
                    // It is ok that there is already a table with this name because "IF NOT EXISTS" was specified
                    return;
                } else {
                    throw new TableAlreadyExistsException();
                }
            }

            boolean foundPk = false;
            for ( ConstraintInformation constraintInformation : constraints ) {
                if ( constraintInformation.type == ConstraintType.PRIMARY ) {
                    if ( foundPk ) {
                        throw new RuntimeException( "More than one primary key has been provided!" );
                    } else {
                        foundPk = true;
                    }
                }
            }
            if ( !foundPk ) {
                throw new RuntimeException( "No primary key has been provided!" );
            }

            if ( stores == null ) {
                // Ask router on which store(s) the table should be placed
                stores = statement.getRouter().createTable( schemaId, statement );
            }

            long tableId = catalog.addTable(
                    tableName,
                    schemaId,
                    statement.getPrepareContext().getCurrentUserId(),
                    TableType.TABLE,
                    true );

            for ( ColumnInformation column : columns ) {
                addColumn( column.name, column.typeInformation, column.collation, column.defaultValue, tableId, column.position, stores, placementType );
            }

            for ( ConstraintInformation constraint : constraints ) {
                addConstraint( constraint.name, constraint.type, constraint.columnNames, tableId );
            }

            //catalog.updateTablePartitionProperties(tableId, partitionProperty);
            CatalogTable catalogTable = catalog.getTable( tableId );

            // Trigger rebuild of schema; triggers schema creation on adapters
            PolySchemaBuilder.getInstance().getCurrent();

            for ( DataStore store : stores ) {
                catalog.addPartitionPlacement(
                        store.getAdapterId(),
                        catalogTable.id,
                        catalogTable.partitionProperty.partitionIds.get( 0 ),
                        PlacementType.AUTOMATIC,
                        null,
                        null );

                store.createTable( statement.getPrepareContext(), catalogTable, catalogTable.partitionProperty.partitionIds );
            }

        } catch ( GenericCatalogException | UnknownColumnException | UnknownCollationException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void addPartitioning( PartitionInformation partitionInfo, List<DataStore> stores, Statement statement ) throws GenericCatalogException, UnknownPartitionTypeException, UnknownColumnException, PartitionGroupNamesNotUniqueException {
        CatalogColumn catalogColumn = catalog.getColumn( partitionInfo.table.id, partitionInfo.columnName );

        PartitionType actualPartitionType = PartitionType.getByName( partitionInfo.typeName );

        // Convert partition names and check whether they are unique
        List<String> sanitizedPartitionGroupNames = partitionInfo.partitionGroupNames
                .stream()
                .map( name -> name.trim().toLowerCase() )
                .collect( Collectors.toList() );
        if ( sanitizedPartitionGroupNames.size() != new HashSet<>( sanitizedPartitionGroupNames ).size() ) {
            throw new PartitionGroupNamesNotUniqueException();
        }

        // Check if specified partitionColumn is even part of the table
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partition group for table: {} with id {} on schema: {} on column: {}", partitionInfo.table.name, partitionInfo.table.id, partitionInfo.table.getSchemaName(), catalogColumn.id );
        }

        CatalogTable unPartitionedTable = catalog.getTable( partitionInfo.table.id );

        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( actualPartitionType );

        // Check whether partition function supports type of partition column
        if ( !partitionManager.supportsColumnOfType( catalogColumn.type ) ) {
            throw new RuntimeException( "The partition function " + actualPartitionType + " does not support columns of type " + catalogColumn.type );
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
        if ( !partitionManager.validatePartitionGroupSetup( partitionInfo.qualifiers, numberOfPartitionGroups, partitionInfo.partitionGroupNames, catalogColumn ) ) {
            throw new RuntimeException( "Partitioning failed for table: " + partitionInfo.table.name );
        }

        // Loop over value to create those partitions with partitionKey to uniquelyIdentify partition
        List<Long> partitionGroupIds = new ArrayList<>();
        for ( int i = 0; i < numberOfPartitionGroups; i++ ) {
            String partitionGroupName;

            // Make last partition unbound partition
            if ( partitionManager.requiresUnboundPartitionGroup() && i == numberOfPartitionGroups - 1 ) {
                partId = catalog.addPartitionGroup(
                        partitionInfo.table.id,
                        "Unbound",
                        partitionInfo.table.schemaId,
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
                    partId = catalog.addPartitionGroup(
                            partitionInfo.table.id,
                            partitionGroupName,
                            partitionInfo.table.schemaId,
                            actualPartitionType,
                            numberOfPartitionsPerGroup,
                            new ArrayList<>(),
                            false );
                } else {
                    partId = catalog.addPartitionGroup(
                            partitionInfo.table.id,
                            partitionGroupName,
                            partitionInfo.table.schemaId,
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
        //catalog.getPartitionGroups( partitionInfo.table.id ).forEach( pg -> partitionIds.forEach( p -> partitionIds.add( p ) ) );
        partitionGroupIds.forEach( pg -> catalog.getPartitions( pg ).forEach( p -> partitionIds.add( p.id ) ) );

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

            int hotPercentageIn = Integer.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageIn().toString() );
            int hotPercentageOut = Integer.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getHotAccessPercentageOut().toString() );

            //Initially distribute partitions as intended in a running system
            long numberOfPartitionsInHot = numberOfPartitions * hotPercentageIn / 100;
            if ( numberOfPartitionsInHot == 0 ) {
                numberOfPartitionsInHot = 1;
            }

            long numberOfPartitionsInCold = numberOfPartitions - numberOfPartitionsInHot;

            // -1 because one partition is already created in COLD
            List<Long> partitionsForHot = new ArrayList<>();
            catalog.getPartitions( partitionGroupIds.get( 0 ) ).forEach( p -> partitionsForHot.add( p.id ) );

            // -1 because one partition is already created in HOT
            for ( int i = 0; i < numberOfPartitionsInHot - 1; i++ ) {
                long tempId;
                tempId = catalog.addPartition( partitionInfo.table.id, partitionInfo.table.schemaId, partitionGroupIds.get( 0 ), partitionInfo.qualifiers.get( 0 ), false );
                partitionIds.add( tempId );
                partitionsForHot.add( tempId );
            }

            catalog.updatePartitionGroup( partitionGroupIds.get( 0 ), partitionsForHot );

            // -1 because one partition is already created in COLD
            List<Long> partitionsForCold = new ArrayList<>();
            catalog.getPartitions( partitionGroupIds.get( 1 ) ).forEach( p -> partitionsForCold.add( p.id ) );

            for ( int i = 0; i < numberOfPartitionsInCold - 1; i++ ) {
                long tempId;
                tempId = catalog.addPartition( partitionInfo.table.id, partitionInfo.table.schemaId, partitionGroupIds.get( 1 ), partitionInfo.qualifiers.get( 1 ), false );
                partitionIds.add( tempId );
                partitionsForCold.add( tempId );
            }

            catalog.updatePartitionGroup( partitionGroupIds.get( 1 ), partitionsForCold );

            partitionProperty = TemperaturePartitionProperty.builder()
                    .partitionType( actualPartitionType )
                    .internalPartitionFunction( PartitionType.valueOf( ((RawTemperaturePartitionInformation) partitionInfo.rawPartitionInformation).getInternalPartitionFunction().toString().toUpperCase() ) )
                    .partitionColumnId( catalogColumn.id )
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
                    .partitionColumnId( catalogColumn.id )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                    .partitionIds( ImmutableList.copyOf( partitionIds ) )
                    .reliesOnPeriodicChecks( false )
                    .build();
        }

        // Update catalog table
        catalog.partitionTable( partitionInfo.table.id, actualPartitionType, catalogColumn.id, numberOfPartitionGroups, partitionGroupIds, partitionProperty );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = partitionInfo.table.primaryKey;
        List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        CatalogColumn pkColumn = catalog.getColumn( pkColumnIds.get( 0 ) );
        // This gets us only one ccp per store (first part of PK)

        boolean fillStores = false;
        if ( stores == null ) {
            stores = new ArrayList<>();
            fillStores = true;
        }
        List<CatalogColumnPlacement> catalogColumnPlacements = catalog.getColumnPlacement( pkColumn.id );
        for ( CatalogColumnPlacement ccp : catalogColumnPlacements ) {
            catalog.updatePartitionGroupsOnDataPlacement( ccp.adapterId, ccp.tableId, partitionGroupIds );
            if ( fillStores ) {
                // Ask router on which store(s) the table should be placed
                Adapter adapter = AdapterManager.getInstance().getAdapter( ccp.adapterId );
                if ( adapter instanceof DataStore ) {
                    stores.add( (DataStore) adapter );
                }
            }
        }

        // Now get the partitioned table, partitionInfo still contains the basic/unpartitioned table.
        CatalogTable partitionedTable = catalog.getTable( partitionInfo.table.id );
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        for ( DataStore store : stores ) {
            for ( long partitionId : partitionIds ) {
                catalog.addPartitionPlacement(
                        store.getAdapterId(),
                        partitionedTable.id,
                        partitionId,
                        PlacementType.AUTOMATIC,
                        null,
                        null );
            }

            // First create new tables
            store.createTable( statement.getPrepareContext(), partitionedTable, partitionedTable.partitionProperty.partitionIds );

            // Copy data from unpartitioned to partitioned
            // Get only columns that are actually on that store
            // Every store of a newly partitioned table, initially will hold all partitions
            List<CatalogColumn> necessaryColumns = new LinkedList<>();
            catalog.getColumnPlacementsOnAdapterPerTable( store.getAdapterId(), partitionedTable.id ).forEach( cp -> necessaryColumns.add( catalog.getColumn( cp.columnId ) ) );

            // Copy data from the old partition to new partitions
            dataMigrator.copyPartitionData(
                    statement.getTransaction(),
                    catalog.getAdapter( store.getAdapterId() ),
                    unPartitionedTable,
                    partitionedTable,
                    necessaryColumns,
                    unPartitionedTable.partitionProperty.partitionIds,
                    partitionedTable.partitionProperty.partitionIds );
        }
        //Remove old tables
        stores.forEach( store -> store.dropTable( statement.getPrepareContext(), unPartitionedTable, unPartitionedTable.partitionProperty.partitionIds ) );
        catalog.deletePartitionGroup( unPartitionedTable.id, unPartitionedTable.schemaId, unPartitionedTable.partitionProperty.partitionGroupIds.get( 0 ) );
    }


    @Override
    public void removePartitioning( CatalogTable partitionedTable, Statement statement ) {
        long tableId = partitionedTable.id;

        if ( log.isDebugEnabled() ) {
            log.debug( "Merging partitions for table: {} with id {} on schema: {}",
                    partitionedTable.name, partitionedTable.id, partitionedTable.getSchemaName() );
        }

        // Need to gather the partitionDistribution before actually merging
        // We need a columnPlacement for every partition
        Map<Long, List<CatalogColumnPlacement>> placementDistribution = new HashMap<>();
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( partitionedTable.partitionProperty.partitionType );
        placementDistribution = partitionManager.getRelevantPlacements( partitionedTable, partitionedTable.partitionProperty.partitionIds, new ArrayList<>( Arrays.asList( -1 ) ) );

        // Update catalog table
        catalog.mergeTable( tableId );

        // Now get the merged table
        CatalogTable mergedTable = catalog.getTable( tableId );

        List<DataStore> stores = new ArrayList<>();
        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = partitionedTable.primaryKey;
        List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        CatalogColumn pkColumn = catalog.getColumn( pkColumnIds.get( 0 ) );
        // This gets us only one ccp per store (first part of PK)

        List<CatalogColumnPlacement> catalogColumnPlacements = catalog.getColumnPlacement( pkColumn.id );
        for ( CatalogColumnPlacement ccp : catalogColumnPlacements ) {
            // Ask router on which store(s) the table should be placed
            Adapter adapter = AdapterManager.getInstance().getAdapter( ccp.adapterId );
            if ( adapter instanceof DataStore ) {
                stores.add( (DataStore) adapter );
            }
        }

        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        boolean firstIteration = true;
        // For merge create only full placements on the used stores. Otherwise partition constraints might not hold
        for ( DataStore store : stores ) {
            // Need to create partitionPlacements first in order to trigger schema creation on PolySchemaBuilder
            catalog.addPartitionPlacement(
                    store.getAdapterId(),
                    mergedTable.id,
                    mergedTable.partitionProperty.partitionIds.get( 0 ),
                    PlacementType.AUTOMATIC,
                    null,
                    null );

            // First create new tables
            store.createTable( statement.getPrepareContext(), mergedTable, mergedTable.partitionProperty.partitionIds );

            // Get only columns that are actually on that store
            List<CatalogColumn> necessaryColumns = new LinkedList<>();
            catalog.getColumnPlacementsOnAdapterPerTable( store.getAdapterId(), mergedTable.id ).forEach( cp -> necessaryColumns.add( catalog.getColumn( cp.columnId ) ) );

            dataMigrator.copySelectiveData(
                    statement.getTransaction(),
                    catalog.getAdapter( store.getAdapterId() ),
                    partitionedTable,
                    mergedTable,
                    necessaryColumns,
                    placementDistribution,
                    mergedTable.partitionProperty.partitionIds );
        }

        // Needs to be separated from loop above. Otherwise we loose data
        for ( DataStore store : stores ) {
            List<Long> partitionIdsOnStore = new ArrayList<>();
            catalog.getPartitionPlacementByTable( store.getAdapterId(), partitionedTable.id ).forEach( p -> partitionIdsOnStore.add( p.partitionId ) );

            // Otherwise everything will be dropped again, leaving the table inaccessible
            partitionIdsOnStore.remove( mergedTable.partitionProperty.partitionIds.get( 0 ) );

            // Drop all partitionedTables (table contains old partitionIds)
            store.dropTable( statement.getPrepareContext(), partitionedTable, partitionIdsOnStore );
        }
        // Loop over **old.partitionIds** to delete all partitions which are part of table
        // Needs to be done separately because partitionPlacements will be recursively dropped in `deletePartitionGroup` but are needed in dropTable
        for ( long partitionGroupId : partitionedTable.partitionProperty.partitionGroupIds ) {
            catalog.deletePartitionGroup( tableId, partitionedTable.schemaId, partitionGroupId );
        }
    }


    private void addColumn( String columnName, ColumnTypeInformation typeInformation, Collation collation, String defaultValue, long tableId, int position, List<DataStore> stores, PlacementType placementType ) throws GenericCatalogException, UnknownCollationException, UnknownColumnException {
        long addedColumnId = catalog.addColumn(
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
        addDefaultValue( defaultValue, addedColumnId );

        for ( DataStore s : stores ) {
            catalog.addColumnPlacement(
                    s.getAdapterId(),
                    addedColumnId,
                    placementType,
                    null,
                    null,
                    null,
                    null );
        }
    }


    @Override
    public void addConstraint( String constraintName, ConstraintType constraintType, List<String> columnNames, long tableId ) throws UnknownColumnException, GenericCatalogException {
        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            CatalogColumn catalogColumn = catalog.getColumn( tableId, columnName );
            columnIds.add( catalogColumn.id );
        }
        if ( constraintType == ConstraintType.PRIMARY ) {
            catalog.addPrimaryKey( tableId, columnIds );
        } else if ( constraintType == ConstraintType.UNIQUE ) {
            if ( constraintName == null ) {
                constraintName = NameGenerator.generateConstraintName();
            }

            catalog.addUniqueConstraint( tableId, constraintName, columnIds );
        }
    }


    @Override
    public void dropSchema( long databaseId, String schemaName, boolean ifExists, Statement statement ) throws SchemaNotExistException, DdlOnSourceException {
        try {
            // Check if there is a schema with this name
            if ( catalog.checkIfExistsSchema( databaseId, schemaName ) ) {
                CatalogSchema catalogSchema = catalog.getSchema( databaseId, schemaName );

                // Drop all tables in this schema
                List<CatalogTable> catalogTables = catalog.getTables( catalogSchema.id, null );
                for ( CatalogTable catalogTable : catalogTables ) {
                    dropTable( catalogTable, statement );
                }

                // Drop schema
                catalog.deleteSchema( catalogSchema.id );
            } else {
                if ( ifExists ) {
                    // This is ok because "IF EXISTS" was specified
                    return;
                } else {
                    throw new SchemaNotExistException();
                }
            }
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropView( CatalogTable catalogView, Statement statement ) throws DdlOnSourceException {
        // Make sure that this is a table of type VIEW
        checkIfViewType( catalogView.tableType );

        // Check if views are dependent from this view
        checkViewDependencies( catalogView );

        catalog.deleteViewDependencies( (CatalogView) catalogView );

        // Delete columns
        for ( Long columnId : catalogView.columnIds ) {
            catalog.deleteColumn( columnId );
        }

        // Delete the view
        catalog.deleteTable( catalogView.id );

        // Rest plan cache and implementation cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void dropTable( CatalogTable catalogTable, Statement statement ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        checkIfTableType( catalogTable.tableType );

        // Check if views dependent on this table
        checkViewDependencies( catalogTable );

        // Check if there are foreign keys referencing this table
        List<CatalogForeignKey> selfRefsToDelete = new LinkedList<>();
        List<CatalogForeignKey> exportedKeys = catalog.getExportedKeys( catalogTable.id );
        if ( exportedKeys.size() > 0 ) {
            for ( CatalogForeignKey foreignKey : exportedKeys ) {
                if ( foreignKey.tableId == catalogTable.id ) {
                    // If this is a self-reference, drop it later.
                    selfRefsToDelete.add( foreignKey );
                } else {
                    throw new PolyphenyDbException( "Cannot drop table '" + catalogTable.getSchemaName() + "." + catalogTable.name + "' because it is being referenced by '" + exportedKeys.get( 0 ).getSchemaName() + "." + exportedKeys.get( 0 ).getTableName() + "'." );
                }
            }
        }

        // Make sure that all adapters are of type store (and not source)
        for ( int storeId : catalogTable.placementsByAdapter.keySet() ) {
            getDataStoreInstance( storeId );
        }

        // Delete all indexes
        for ( CatalogIndex index : catalog.getIndexes( catalogTable.id, false ) ) {
            if ( index.location == 0 ) {
                // Delete polystore index
                IndexManager.getInstance().deleteIndex( index );
            } else {
                // Delete index on store
                AdapterManager.getInstance().getStore( index.location ).dropIndex( statement.getPrepareContext(), index );
            }
            // Delete index in catalog
            catalog.deleteIndex( index.id );
        }

        // Delete data from the stores and remove the column placement
        catalog.flagTableForDeletion( catalogTable.id, true );
        for ( int storeId : catalogTable.placementsByAdapter.keySet() ) {
            // Delete table on store
            List<Long> partitionIdsOnStore = new ArrayList<>();
            catalog.getPartitionPlacementByTable( storeId, catalogTable.id ).forEach( p -> partitionIdsOnStore.add( p.partitionId ) );

            AdapterManager.getInstance().getStore( storeId ).dropTable( statement.getPrepareContext(), catalogTable, partitionIdsOnStore );
            // Inform routing
            statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapterPerTable( storeId, catalogTable.id ) );
            // Delete column placement in catalog
            for ( Long columnId : catalogTable.columnIds ) {
                if ( catalog.checkIfExistsColumnPlacement( storeId, columnId ) ) {
                    catalog.deleteColumnPlacement( storeId, columnId );
                }
            }
        }

        // Delete the self-referencing foreign keys
        try {
            for ( CatalogForeignKey foreignKey : selfRefsToDelete ) {
                catalog.deleteForeignKey( foreignKey.id );
            }
        } catch ( GenericCatalogException e ) {
            catalog.flagTableForDeletion( catalogTable.id, true );
            throw new PolyphenyDbContextException( "Exception while deleting self-referencing foreign key constraints.", e );
        }

        // Delete indexes of this table
        List<CatalogIndex> indexes = catalog.getIndexes( catalogTable.id, false );
        for ( CatalogIndex index : indexes ) {
            catalog.deleteIndex( index.id );
            IndexManager.getInstance().deleteIndex( index );
        }

        // Delete keys and constraints
        try {
            // Remove primary key
            catalog.deletePrimaryKey( catalogTable.id );
            // Delete all foreign keys of the table
            List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( catalogTable.id );
            for ( CatalogForeignKey foreignKey : foreignKeys ) {
                catalog.deleteForeignKey( foreignKey.id );
            }
            // Delete all constraints of the table
            for ( CatalogConstraint constraint : catalog.getConstraints( catalogTable.id ) ) {
                catalog.deleteConstraint( constraint.id );
            }
        } catch ( GenericCatalogException e ) {
            catalog.flagTableForDeletion( catalogTable.id, true );
            throw new PolyphenyDbContextException( "Exception while dropping keys.", e );
        }

        // Delete columns
        for ( Long columnId : catalogTable.columnIds ) {
            catalog.deleteColumn( columnId );
        }

        // Delete the table
        catalog.deleteTable( catalogTable.id );

        // Rest plan cache and implementation cache
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void truncate( CatalogTable catalogTable, Statement statement ) {
        // Make sure that the table can be modified
        if ( !catalogTable.modifiable ) {
            throw new RuntimeException( "Unable to modify a read-only table!" );
        }

        //  Execute truncate on all placements
        catalogTable.placementsByAdapter.forEach( ( adapterId, placements ) -> {
            AdapterManager.getInstance().getAdapter( adapterId ).truncate( statement.getPrepareContext(), catalogTable );
        } );
    }


    @Override
    public void dropFunction() {
        throw new RuntimeException( "Not supported yet" );
    }


    @Override
    public void setOption() {
        throw new RuntimeException( "Not supported yet" );
    }


    @Override
    public void createType() {
        throw new RuntimeException( "Not supported yet" );
    }


    @Override
    public void dropType() {
        throw new RuntimeException( "Not supported yet" );
    }

}
