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

import static org.reflections.Reflections.log;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.polypheny.db.catalog.entity.CatalogMaterialized;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
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
import org.polypheny.db.ddl.exception.PartitionNamesNotUniqueException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementIsPrimaryException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.ddl.exception.UnknownIndexMethodException;
import org.polypheny.db.materializedView.MaterializedManager;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.LogicalView;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;


public class DdlManagerImpl extends DdlManager {

    private final Catalog catalog;


    public DdlManagerImpl( Catalog catalog ) {
        this.catalog = catalog;
    }


    private void checkIfTableType( TableType tableType ) throws DdlOnSourceException {
        if ( tableType != TableType.TABLE && tableType != TableType.MATERIALIZEDVIEW ) {
            throw new DdlOnSourceException();
        }
    }


    private void checkIfViewType( TableType tableType ) throws DdlOnSourceException {
        if ( tableType != TableType.VIEW ) {
            throw new DdlOnSourceException();
        }
    }


    private void checkIfMaterializedViewType( TableType tableType ) throws DdlOnSourceException {
        if ( tableType != TableType.MATERIALIZEDVIEW ) {
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
                            null );
                    catalog.updateColumnPlacementPhysicalPosition( adapter.getAdapterId(), columnId, exportedColumn.physicalPosition );
                    if ( exportedColumn.primary ) {
                        primaryKeyColIds.add( columnId );
                    }
                }
                try {
                    catalog.addPrimaryKey( tableId, primaryKeyColIds );
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
                statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapter( catalogAdapter.id, table.id ) );
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
        if ( catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).size() != 1 ) {
            throw new RuntimeException( "The table has an unexpected number of placements!" );
        }

        int adapterId = catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).get( 0 ).adapterId;
        DataSource dataSource = (DataSource) AdapterManager.getInstance().getAdapter( adapterId );

        String physicalTableName = catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).get( 0 ).physicalTableName;
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
        for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapter( adapterId, catalogTable.id ) ) {
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
                null );

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
                    null );
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
    public void addPlacement( CatalogTable catalogTable, List<Long> columnIds, List<Integer> partitionIds, List<String> partitionNames, DataStore dataStore, Statement statement ) throws PlacementAlreadyExistsException {
        List<CatalogColumn> addedColumns = new LinkedList<>();

        List<Long> tempPartitionList = new ArrayList<>();

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
        if ( catalogTable.isPartitioned ) {
            boolean isDataPlacementPartitioned = false;
            long tableId = catalogTable.id;
            // Needed to ensure that column placements on the same store contain all the same partitions
            // Check if this column placement is the first on the data placement
            // If this returns null this means that this is the first placement and partition list can therefore be specified
            List<Long> currentPartList = new ArrayList<>();
            currentPartList = catalog.getPartitionsOnDataPlacement( dataStore.getAdapterId(), catalogTable.id );

            isDataPlacementPartitioned = !currentPartList.isEmpty();

            if ( !partitionIds.isEmpty() && partitionNames.isEmpty() ) {

                // Abort if a manual partitionList has been specified even though the data placement has already been partitioned
                if ( isDataPlacementPartitioned ) {
                    throw new RuntimeException( "WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                            + dataStore.getAdapterName() + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead" );
                }

                log.debug( "Table is partitioned and concrete partitionList has been specified " );
                // First convert specified index to correct partitionId
                for ( int partitionId : partitionIds ) {
                    // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionList.add( catalogTable.partitionIds.get( partitionId ) );
                    } catch ( IndexOutOfBoundsException e ) {
                        throw new RuntimeException( "Specified Partition-Index: '" + partitionId + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalogTable.numPartitions + " partitions" );
                    }
                }
            } else if ( !partitionNames.isEmpty() && partitionIds.isEmpty() ) {

                if ( isDataPlacementPartitioned ) {
                    throw new RuntimeException( "WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                            + dataStore.getAdapterName() + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead" );
                }

                List<CatalogPartition> catalogPartitions = catalog.getPartitions( tableId );
                for ( String partitionName : partitionNames ) {
                    boolean isPartOfTable = false;
                    for ( CatalogPartition catalogPartition : catalogPartitions ) {
                        if ( partitionName.equals( catalogPartition.partitionName.toLowerCase() ) ) {
                            tempPartitionList.add( catalogPartition.id );
                            isPartOfTable = true;
                            break;
                        }
                    }
                    if ( !isPartOfTable ) {
                        throw new RuntimeException( "Specified Partition-Name: '" + partitionName + "' is not part of table '"
                                + catalogTable.name + "'. Available partitions: " + String.join( ",", catalog.getPartitionNames( tableId ) ) );

                    }
                }
            }
            // Simply Place all partitions on placement since nothing has been specified
            else if ( partitionIds.isEmpty() && partitionNames.isEmpty() ) {
                log.debug( "Table is partitioned and concrete partitionList has NOT been specified " );

                if ( isDataPlacementPartitioned ) {
                    // If DataPlacement already contains partitions then create new placement with same set of partitions.
                    tempPartitionList = currentPartList;
                } else {
                    tempPartitionList = catalogTable.partitionIds;
                }
            }
        }

        // Create column placements
        for ( long cid : columnIds ) {
            catalog.addColumnPlacement(
                    dataStore.getAdapterId(),
                    cid,
                    PlacementType.MANUAL,
                    null,
                    null,
                    null,
                    tempPartitionList );
            addedColumns.add( catalog.getColumn( cid ) );
        }
        //Check if placement includes primary key columns
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
                        tempPartitionList );
                addedColumns.add( catalog.getColumn( cid ) );
            }
        }
        // Create table on store
        dataStore.createTable( statement.getPrepareContext(), catalogTable );
        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( dataStore.getAdapterId() ), addedColumns );
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
            List<CatalogColumnPlacement> oldPkPlacements = catalog.getColumnPlacements( pkColumnId );
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
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapter( storeInstance.getAdapterId(), catalogTable.id ) ) {
            List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacements( placement.columnId );
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
        storeInstance.dropTable( statement.getPrepareContext(), catalogTable );
        // Inform routing
        statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapter( storeInstance.getAdapterId(), catalogTable.id ) );
        // Delete placement in the catalog
        List<CatalogColumnPlacement> placements = catalog.getColumnPlacementsOnAdapter( storeInstance.getAdapterId(), catalogTable.id );
        for ( CatalogColumnPlacement placement : placements ) {
            catalog.deleteColumnPlacement( storeInstance.getAdapterId(), placement.columnId );
        }

        // Remove All
        catalog.deletePartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id );
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
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacements( catalogColumn.id ) ) {
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
    public void modifyColumnPlacement( CatalogTable catalogTable, List<Long> columnIds, List<Integer> partitionIds, List<String> partitionNames, DataStore storeInstance, Statement statement ) throws PlacementNotExistsException, IndexPreventsRemovalException, LastPlacementException {
        // Check whether this placement already exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw new PlacementNotExistsException();
        }

        //check if views are dependent from this view
        checkViewDependencies( catalogTable );

        // Which columns to remove
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapter( storeInstance.getAdapterId(), catalogTable.id ) ) {
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
                    List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacements( placement.columnId );
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

        List<Long> tempPartitionList = new ArrayList<>();
        // Select partitions to create on this placement
        if ( catalogTable.isPartitioned ) {
            long tableId = catalogTable.id;
            // If index partitions are specified
            if ( !partitionIds.isEmpty() && partitionNames.isEmpty() ) {
                // First convert specified index to correct partitionId
                for ( int partitionId : partitionIds ) {
                    // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionList.add( catalogTable.partitionIds.get( partitionId ) );
                    } catch ( IndexOutOfBoundsException e ) {
                        throw new RuntimeException( "Specified Partition-Index: '" + partitionId + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalogTable.numPartitions + " partitions" );
                    }
                }
                catalog.updatePartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id, tempPartitionList );
            }
            // If name partitions are specified
            else if ( !partitionNames.isEmpty() && partitionIds.isEmpty() ) {
                List<CatalogPartition> catalogPartitions = catalog.getPartitions( tableId );
                for ( String partitionName : partitionNames ) {
                    boolean isPartOfTable = false;
                    for ( CatalogPartition catalogPartition : catalogPartitions ) {
                        if ( partitionName.equals( catalogPartition.partitionName.toLowerCase() ) ) {
                            tempPartitionList.add( catalogPartition.id );
                            isPartOfTable = true;
                            break;
                        }
                    }
                    if ( !isPartOfTable ) {
                        throw new RuntimeException( "Specified partition name: '" + partitionName + "' is not part of table '"
                                + catalogTable.name + "'. Available partitions: " + String.join( ",", catalog.getPartitionNames( tableId ) ) );
                    }
                }
                catalog.updatePartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id, tempPartitionList );
            }
        }

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
                        tempPartitionList );
                // Add column on store
                storeInstance.addColumn( statement.getPrepareContext(), catalogTable, catalog.getColumn( cid ) );
                // Add to list of columns for which we need to copy data
                addedColumns.add( catalog.getColumn( cid ) );
            }
        }
        // Copy the data to the newly added column placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        if ( addedColumns.size() > 0 ) {
            dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( storeInstance.getAdapterId() ), addedColumns );
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
            dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( storeInstance.getAdapterId() ), ImmutableList.of( catalogColumn ) );
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
        List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacements( catalogColumn.id );
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

        prepareView( relNode );
        RelDataType fieldList = relNode.getRowType();

        List<ColumnInformation> columns = getColumnInformation( projectedColumns, fieldList );

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


    @Override
    public void createMaterializedView( String viewName, long schemaId, RelRoot relRoot, boolean replace, Statement statement, List<DataStore> stores, PlacementType placementType, List<String> projectedColumns, MaterializedCriteria materializedCriteria ) throws TableAlreadyExistsException, GenericCatalogException, UnknownColumnException {
        if ( catalog.checkIfExistsTable( schemaId, viewName ) ) {
            throw new TableAlreadyExistsException();
        }

        if ( stores == null ) {
            // Ask router on which store(s) the table should be placed
            stores = statement.getRouter().createTable( schemaId, statement );
        }

        prepareView( relRoot.rel );
        RelDataType fieldList = relRoot.rel.getRowType();

        List<ColumnInformation> columns = getColumnInformation( projectedColumns, fieldList );

        Map<Long, List<Long>> underlyingTables = new HashMap<>();
        long tableId = catalog.addMaterializedView(
                viewName,
                schemaId,
                statement.getPrepareContext().getCurrentUserId(),
                TableType.MATERIALIZEDVIEW,
                false,
                relRoot.rel,
                relRoot.collation,
                findUnderlyingTablesOfView( relRoot.rel, underlyingTables, fieldList ),
                fieldList,
                materializedCriteria
        );

        Map<Integer, List<CatalogColumn>> addedColumns = new HashMap<>();

        for ( ColumnInformation column : columns ) {
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

            for ( DataStore s : stores ) {
                int adapterId = s.getAdapterId();
                catalog.addColumnPlacement(
                        adapterId,
                        columnId,
                        placementType,
                        null,
                        null,
                        null,
                        null );

                List<CatalogColumn> catalogColumns;
                if ( addedColumns.containsKey( adapterId ) ) {
                    catalogColumns = addedColumns.get( adapterId );
                } else {
                    catalogColumns = new ArrayList<>();
                }
                catalogColumns.add( catalog.getColumn( columnId ) );
                addedColumns.put( adapterId, catalogColumns );
            }

        }

        CatalogMaterialized catalogMaterialized = (CatalogMaterialized) catalog.getTable( tableId );
        for ( DataStore store : stores ) {
            store.createTable( statement.getPrepareContext(), catalogMaterialized );
        }

        MaterializedManager materializedManager = MaterializedManager.getInstance();
        materializedManager.addData( statement.getTransaction(), stores, addedColumns, relRoot, catalogMaterialized );


    }


    private List<ColumnInformation> getColumnInformation( List<String> projectedColumns, RelDataType fieldList ) {
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
        return columns;
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
    public void createTable( long schemaId, String tableName, List<ColumnInformation> columns, List<ConstraintInformation> constraints, boolean ifNotExists, List<DataStore> stores, PlacementType placementType, Statement statement ) throws TableAlreadyExistsException, ColumnNotExistsException, UnknownPartitionTypeException {
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

            CatalogTable catalogTable = catalog.getTable( tableId );
            for ( DataStore store : stores ) {
                store.createTable( statement.getPrepareContext(), catalogTable );
            }

        } catch ( GenericCatalogException | UnknownColumnException | UnknownCollationException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void addPartition( PartitionInformation partitionInfo ) throws GenericCatalogException, UnknownPartitionTypeException, UnknownColumnException, PartitionNamesNotUniqueException {
        CatalogColumn catalogColumn = catalog.getColumn( partitionInfo.table.id, partitionInfo.columnName );

        Catalog.PartitionType actualPartitionType = Catalog.PartitionType.getByName( partitionInfo.typeName );

        // Convert partition names and check whether they are unique
        List<String> sanitizedPartitionNames = partitionInfo.partitionNames
                .stream()
                .map( name -> name.trim().toLowerCase() )
                .collect( Collectors.toList() );
        if ( sanitizedPartitionNames.size() != new HashSet<>( sanitizedPartitionNames ).size() ) {
            throw new PartitionNamesNotUniqueException();
        }

        // Check if specified partitionColumn is even part of the table
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partition for table: {} with id {} on schema: {} on column: {}", partitionInfo.table.name, partitionInfo.table.id, partitionInfo.table.getSchemaName(), catalogColumn.id );
        }

        // Get partition manager
        PartitionManagerFactory partitionManagerFactory = new PartitionManagerFactory();
        PartitionManager partitionManager = partitionManagerFactory.getInstance( actualPartitionType );

        // Check whether partition function supports type of partition column
        if ( !partitionManager.supportsColumnOfType( catalogColumn.type ) ) {
            throw new RuntimeException( "The partition function " + actualPartitionType + " does not support columns of type " + catalogColumn.type );
        }

        int numberOfPartitions = partitionInfo.numberOf;
        // Calculate how many partitions exist if partitioning is applied.
        long partId;
        if ( partitionInfo.partitionNames.size() >= 2 && partitionInfo.numberOf == 0 ) {
            numberOfPartitions = partitionInfo.partitionNames.size();
        }

        if ( partitionManager.requiresUnboundPartition() ) {
            // Because of the implicit unbound partition
            numberOfPartitions = partitionInfo.partitionNames.size();
            numberOfPartitions += 1;
        }

        // Validate partition setup
        if ( !partitionManager.validatePartitionSetup( partitionInfo.qualifiers, numberOfPartitions, partitionInfo.partitionNames, catalogColumn ) ) {
            throw new RuntimeException( "Partitioning failed for table: " + partitionInfo.table.name );
        }

        // Loop over value to create those partitions with partitionKey to uniquelyIdentify partition
        List<Long> partitionIds = new ArrayList<>();
        for ( int i = 0; i < numberOfPartitions; i++ ) {
            String partitionName;

            // Make last partition unbound partition
            if ( partitionManager.requiresUnboundPartition() && i == numberOfPartitions - 1 ) {
                partId = catalog.addPartition(
                        partitionInfo.table.id,
                        "Unbound",
                        partitionInfo.table.schemaId,
                        partitionInfo.table.ownerId,
                        actualPartitionType,
                        new ArrayList<>(),
                        true );
            } else {
                // If no names have been explicitly defined
                if ( partitionInfo.partitionNames.isEmpty() ) {
                    partitionName = "part_" + i;
                } else {
                    partitionName = partitionInfo.partitionNames.get( i );
                }

                // Mainly needed for HASH
                if ( partitionInfo.qualifiers.isEmpty() ) {
                    partId = catalog.addPartition(
                            partitionInfo.table.id,
                            partitionName,
                            partitionInfo.table.schemaId,
                            partitionInfo.table.ownerId,
                            actualPartitionType,
                            new ArrayList<>(),
                            false );
                } else {
                    //partId = catalog.addPartition( tableId, partitionName, old.schemaId, old.ownerId, partitionType, new ArrayList<>( Collections.singletonList( partitionQualifiers.get( i ) ) ), false );
                    partId = catalog.addPartition(
                            partitionInfo.table.id,
                            partitionName,
                            partitionInfo.table.schemaId,
                            partitionInfo.table.ownerId,
                            actualPartitionType,
                            partitionInfo.qualifiers.get( i ),
                            false );
                }
            }
            partitionIds.add( partId );
        }

        // Update catalog table
        catalog.partitionTable( partitionInfo.table.id, actualPartitionType, catalogColumn.id, numberOfPartitions, partitionIds );

        // Get primary key of table and use PK to find all DataPlacements of table
        long pkid = partitionInfo.table.primaryKey;
        List<Long> pkColumnIds = catalog.getPrimaryKey( pkid ).columnIds;
        // Basically get first part of PK even if its compound of PK it is sufficient
        CatalogColumn pkColumn = catalog.getColumn( pkColumnIds.get( 0 ) );
        // This gets us only one ccp per store (first part of PK)
        for ( CatalogColumnPlacement ccp : catalog.getColumnPlacements( pkColumn.id ) ) {
            catalog.updatePartitionsOnDataPlacement( ccp.adapterId, ccp.tableId, partitionIds );
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
    public void dropMaterializedView( CatalogTable materializedView, Statement statement ) throws DdlOnSourceException {
        // Make sure that this is a table of type VIEW
        checkIfMaterializedViewType( materializedView.tableType );

        // Check if views are dependent from this view
        checkViewDependencies( materializedView );

        catalog.deleteViewDependencies( (CatalogView) materializedView );

        dropTable( materializedView, statement );
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
            AdapterManager.getInstance().getStore( storeId ).dropTable( statement.getPrepareContext(), catalogTable );
            // Inform routing
            statement.getRouter().dropPlacements( catalog.getColumnPlacementsOnAdapter( storeId, catalogTable.id ) );
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
