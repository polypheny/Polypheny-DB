/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;


/**
 * This helper class should serve as a base when implementation-testing different functionalities of
 * Polypheny, which use the catalog.
 * By extending and only implementing the minimal function of the catalog it should
 * provide a clean testing setup
 */
public abstract class MockCatalog extends Catalog {

    @Override
    public void commit() throws NoTablePrimaryKeyException {
        throw new NotImplementedException();
    }


    @Override
    public int addUser( String name, String password ) {
        throw new NotImplementedException();
    }


    @Override
    public void setUserSchema( int userId, long schemaId ) {
        throw new NotImplementedException();
    }


    @Override
    public void rollback() {
        throw new NotImplementedException();
    }


    @Override
    public void validateColumns() {
        throw new NotImplementedException();
    }


    @Override
    public void restoreColumnPlacements( Transaction transaction ) {
        throw new NotImplementedException();
    }


    @Override
    public long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName ) {
        throw new NotImplementedException();
    }


    @Override
    public Map<Long, AlgNode> getNodeInfo() {
        throw new NotImplementedException();
    }


    @Override
    public Map<Long, AlgDataType> getAlgTypeInfo() {
        throw new NotImplementedException();
    }


    @Override
    public void restoreViews( Transaction transaction ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteDatabase( long databaseId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogDatabase> getDatabases( Pattern pattern ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogDatabase getDatabase( String databaseName ) throws UnknownDatabaseException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogDatabase getDatabase( long databaseId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogSchema getSchema( long schemaId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogSchema getSchema( String databaseName, String schemaName ) throws UnknownSchemaException, UnknownDatabaseException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogSchema getSchema( long databaseId, String schemaName ) throws UnknownSchemaException {
        throw new NotImplementedException();
    }


    @Override
    public long addSchema( String name, long databaseId, int ownerId, SchemaType schemaType ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsSchema( long databaseId, String schemaName ) {
        throw new NotImplementedException();
    }


    @Override
    public void renameSchema( long schemaId, String name ) {
        throw new NotImplementedException();
    }


    @Override
    public void setSchemaOwner( long schemaId, long ownerId ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteSchema( long schemaId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException, UnknownDatabaseException, UnknownSchemaException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogTable getTableFromPartition( long partitionId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogTable getTable( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException {
        throw new NotImplementedException();
    }


    @Override
    public long addTable( String name, long schemaId, int ownerId, TableType tableType, boolean modifiable ) {
        throw new NotImplementedException();
    }


    @Override
    public long addView( String name, long schemaId, int ownerId, TableType tableType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, String query, QueryLanguage language ) {
        throw new NotImplementedException();
    }


    @Override
    public long addMaterializedView( String name, long schemaId, int ownerId, TableType tableType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteViewDependencies( CatalogView catalogView ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsTable( long schemaId, String tableName ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsTable( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void renameTable( long tableId, String name ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteTable( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void setTableOwner( long tableId, int ownerId ) {
        throw new NotImplementedException();
    }


    @Override
    public void setPrimaryKey( long tableId, Long keyId ) {
        throw new NotImplementedException();
    }


    @Override
    public void addColumnPlacement( int adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteColumnPlacement( int adapterId, long columnId, boolean columnOnly ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogColumnPlacement getColumnPlacement( int adapterId, long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsColumnPlacement( int adapterId, long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacement( long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterSortedByPhysicalPosition( int storeId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogKey> getKeys() {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogKey> getTableKeys( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( int adapterId, long schemaId ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateColumnPlacementType( int adapterId, long columnId, PlacementType placementType ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateColumnPlacementPhysicalPosition( int adapterId, long columnId, long position ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateColumnPlacementPhysicalPosition( int adapterId, long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateColumnPlacementPhysicalNames( int adapterId, long columnId, String physicalSchemaName, String physicalColumnName, boolean updatePhysicalColumnPosition ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateMaterializedViewRefreshTime( long materializedId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumn> getColumns( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumn> getColumns( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogColumn getColumn( long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogColumn getColumn( long tableId, String columnName ) throws UnknownColumnException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownDatabaseException, UnknownTableException {
        throw new NotImplementedException();
    }


    @Override
    public long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) {
        throw new NotImplementedException();
    }


    @Override
    public void renameColumn( long columnId, String name ) {
        throw new NotImplementedException();
    }


    @Override
    public void setColumnPosition( long columnId, int position ) {
        throw new NotImplementedException();
    }


    @Override
    public void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer precision, Integer dimension, Integer cardinality ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void setNullable( long columnId, boolean nullable ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void setCollation( long columnId, Collation collation ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteColumn( long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public void setDefaultValue( long columnId, PolyType type, String defaultValue ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteDefaultValue( long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean isPrimaryKey( long keyId ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean isForeignKey( long keyId ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean isIndex( long keyId ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean isConstraint( long keyId ) {
        throw new NotImplementedException();
    }


    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogIndex> getIndexes( CatalogKey key ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogIndex> getForeignKeys( CatalogKey key ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogConstraint> getConstraints( CatalogKey key ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) throws UnknownConstraintException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws UnknownForeignKeyException {
        throw new NotImplementedException();
    }


    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogIndex getIndex( long tableId, String indexName ) throws UnknownIndexException {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogIndex getIndex( long indexId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogIndex> getIndexes() {
        throw new NotImplementedException();
    }


    @Override
    public long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, int location, IndexType type, String indexName ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void setIndexPhysicalName( long indexId, String physicalName ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteIndex( long indexId ) {
        throw new NotImplementedException();
    }


    @Override
    public void deletePrimaryKey( long tableId ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void deleteForeignKey( long foreignKeyId ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void deleteConstraint( long constraintId ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogUser getUser( String userName ) throws UnknownUserException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogUser getUser( int userId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        throw new NotImplementedException();
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogAdapter getAdapter( int adapterId ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsAdapter( int adapterId ) {
        throw new NotImplementedException();
    }


    @Override
    public int addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateAdapterSettings( int adapterId, Map<String, String> newSettings ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteAdapter( int adapterId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogQueryInterface> getQueryInterfaces() {
        throw new NotImplementedException();
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogQueryInterface getQueryInterface( int ifaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public int addQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteQueryInterface( int ifaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public long addPartitionGroup( long tableId, String partitionGroupName, long schemaId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    @Override
    public void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogPartitionGroup getPartitionGroup( long partitionGroupId ) {
        throw new NotImplementedException();
    }


    @Override
    public void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitionGroups, List<Long> partitionGroupIds, PartitionProperty partitionProperty ) {
        throw new NotImplementedException();
    }


    @Override
    public void mergeTable( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public List<String> getPartitionGroupNames( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<Long> getPartitionGroupsOnDataPlacement( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<Long> getPartitionGroupsIndexOnDataPlacement( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void flagTableForDeletion( long tableId, boolean flag ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean isTableFlaggedForDeletion( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void close() {
        throw new NotImplementedException();
    }


    @Override
    public void clear() {
        throw new NotImplementedException();
    }


    /**
     * Adds a partition to the catalog
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionGroupId partitionGroupId where the partition should be initially added to
     * @return The id of the created partition
     */
    @Override
    public long addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
        throw new NotImplementedException();
    }


    /**
     * Deletes a single partition and all references.
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionId The partitionId to be deleted
     */
    @Override
    public void deletePartition( long tableId, long schemaId, long partitionId ) {
        throw new NotImplementedException();
    }


    /**
     * Get a partition object by its unique id
     *
     * @param partitionId The unique id of the partition
     * @return A catalog partition
     */
    @Override
    public CatalogPartition getPartition( long partitionId ) {
        throw new NotImplementedException();
    }


    /**
     * Updates partitionProperties on table
     *
     * @param tableId Table to be partitioned
     * @param partitionProperty Partition properties
     */
    @Override
    public void updateTablePartitionProperties( long tableId, PartitionProperty partitionProperty ) {
        throw new NotImplementedException();
    }


    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param partitionGroupId Table to be queried
     * @return list of all partitions on this table
     */
    @Override
    public List<CatalogPartition> getPartitions( long partitionGroupId ) {
        throw new NotImplementedException();
    }


    /**
     * Get all partitions of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all partitions of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns catalog/src/test/java/org/polypheny/db/test/CatalogTest.javaall.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogPartition> getPartitions( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        throw new NotImplementedException();
    }


    /**
     * Get all partitions of a DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionIds
     */
    @Override
    public List<Long> getPartitionsOnDataPlacement( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    /**
     * Adds a placement for a partition.
     *
     * @param adapterId The adapter on which the table should be placed on
     * @param placementType The type of placement
     * @param physicalSchemaName The schema name on the adapter
     * @param physicalTableName The table name on the adapter
     */
    @Override
    public void addPartitionPlacement( int adapterId, long tableId, long partitionId, PlacementType placementType, String physicalSchemaName, String physicalTableName ) {
        throw new NotImplementedException();
    }


    /**
     * Change physical names of a partition placement.
     *
     * @param adapterId The id of the adapter
     * @param partitionId The id of the partition
     * @param physicalSchemaName The physical schema name
     * @param physicalTableName The physical table name
     */
    @Override
    public void updatePartitionPlacementPhysicalNames( int adapterId, long partitionId, String physicalSchemaName, String physicalTableName ) {
        throw new NotImplementedException();
    }


    /**
     * Deletes a placement for a partition.
     *
     * @param adapterId The adapter on which the table should be placed on
     */
    @Override
    public void deletePartitionPlacement( int adapterId, long partitionId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogPartitionPlacement getPartitionPlacement( int adapterId, long partitionId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( int adapterId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsPartitionPlacement( int adapterId, long partitionId ) {
        throw new NotImplementedException();
    }


    @Override
    public void removeTableFromPeriodicProcessing( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void addTableToPeriodicProcessing( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogTable> getTablesForPeriodicProcessing() {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogPartition> getPartitionsByTable( long tableId ) {
        throw new NotImplementedException();
    }


    /**
     * Updates the specified partition group with the attached partitionIds
     *
     * @param partitionIds List of new partitionIds
     */
    @Override
    public void updatePartitionGroup( long partitionGroupId, List<Long> partitionIds ) {
        throw new NotImplementedException();
    }


    @Override
    public void addPartitionToGroup( long partitionGroupId, Long partitionId ) {
        throw new NotImplementedException();
    }


    @Override
    public void removePartitionFromGroup( long partitionGroupId, Long partitionId ) {
        throw new NotImplementedException();
    }


    /**
     * Assign the partition to a new partitionGroup
     */
    @Override
    public void updatePartition( long partitionId, Long partitionGroupId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogDataPlacement getDataPlacement( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacements( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role ) {
        throw new NotImplementedException();
    }


    @Override
    public void addDataPlacement( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public CatalogDataPlacement addDataPlacementIfNotExists( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    protected void modifyDataPlacement( int adapterId, long tableId, CatalogDataPlacement catalogDataPlacement ) {
        throw new NotImplementedException();
    }


    @Override
    public void removeDataPlacement( int adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    protected void addSingleDataPlacementToTable( Integer adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    protected void removeSingleDataPlacementFromTable( Integer adapterId, long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateDataPlacementsOnTable( long tableId, List<Integer> newDataPlacements ) {
        throw new NotImplementedException();
    }


    @Override
    protected void addColumnsToDataPlacement( int adapterId, long tableId, List<Long> columnIds ) {
        throw new NotImplementedException();
    }


    @Override
    protected void removeColumnsFromDataPlacement( int adapterId, long tableId, List<Long> columnIds ) {
        throw new NotImplementedException();
    }


    @Override
    protected void addPartitionsToDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {
        throw new NotImplementedException();
    }


    @Override
    protected void removePartitionsFromDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateDataPlacement( int adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds ) {
        throw new NotImplementedException();
    }


    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getPartitionPlacementsByAdapter( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getPartitionGroupsByAdapter( long tableId ) {
        return null;
    }


    @Override
    public boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogDataPlacement> getAllFullDataPlacements( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public long getPartitionGroupByPartition( long partitionId ) {
        throw new NotImplementedException();
    }

}
