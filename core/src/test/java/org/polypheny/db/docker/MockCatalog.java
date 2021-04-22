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

package org.polypheny.db.docker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
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
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;


/**
 * This is a bare-bone catalog which allows to mock register adapters
 * which then can be retrieved while testing
 */
public class MockCatalog extends Catalog {

    int i = 0;
    HashMap<Integer, CatalogAdapter> adapters = new HashMap<>();


    @Override
    public void commit() throws NoTablePrimaryKeyException {

    }


    @Override
    public void rollback() {

    }


    @Override
    public void validateColumns() {

    }


    @Override
    public void restoreColumnPlacements( Transaction transaction ) {

    }


    @Override
    public long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName ) {
        return 0;
    }


    @Override
    public void deleteDatabase( long databaseId ) {

    }


    @Override
    public List<CatalogDatabase> getDatabases( Pattern pattern ) {
        return null;
    }


    @Override
    public CatalogDatabase getDatabase( String databaseName ) throws UnknownDatabaseException {
        return null;
    }


    @Override
    public CatalogDatabase getDatabase( long databaseId ) {
        return null;
    }


    @Override
    public List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) {
        return null;
    }


    @Override
    public List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern ) {
        return null;
    }


    @Override
    public CatalogSchema getSchema( long schemaId ) {
        return null;
    }


    @Override
    public CatalogSchema getSchema( String databaseName, String schemaName ) throws UnknownSchemaException, UnknownDatabaseException {
        return null;
    }


    @Override
    public CatalogSchema getSchema( long databaseId, String schemaName ) throws UnknownSchemaException {
        return null;
    }


    @Override
    public long addSchema( String name, long databaseId, int ownerId, SchemaType schemaType ) {
        return 0;
    }


    @Override
    public boolean checkIfExistsSchema( long databaseId, String schemaName ) {
        return false;
    }


    @Override
    public void renameSchema( long schemaId, String name ) {

    }


    @Override
    public void setSchemaOwner( long schemaId, long ownerId ) {

    }


    @Override
    public void deleteSchema( long schemaId ) {

    }


    @Override
    public List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException, UnknownDatabaseException, UnknownSchemaException {
        return null;
    }


    @Override
    public List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public CatalogTable getTable( long tableId ) {
        return null;
    }


    @Override
    public CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException {
        return null;
    }


    @Override
    public CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException {
        return null;
    }


    @Override
    public long addTable( String name, long schemaId, int ownerId, TableType tableType, boolean modifiable, String definition ) {
        return 0;
    }


    @Override
    public boolean checkIfExistsTable( long schemaId, String tableName ) {
        return false;
    }


    @Override
    public void renameTable( long tableId, String name ) {

    }


    @Override
    public void deleteTable( long tableId ) {

    }


    @Override
    public void setTableOwner( long tableId, int ownerId ) {

    }


    @Override
    public void setPrimaryKey( long tableId, Long keyId ) {

    }


    @Override
    public void addColumnPlacement( int adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName, List<Long> partitionIds ) {

    }


    @Override
    public void deleteColumnPlacement( int adapterId, long columnId ) {

    }


    @Override
    public CatalogColumnPlacement getColumnPlacement( int adapterId, long columnId ) {
        return null;
    }


    @Override
    public boolean checkIfExistsColumnPlacement( int adapterId, long columnId ) {
        return false;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacements( long columnId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterSortedByPhysicalPosition( int storeId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        return null;
    }


    @Override
    public List<CatalogKey> getKeys() {
        return null;
    }


    @Override
    public List<CatalogKey> getTableKeys( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( int adapterId, long schemaId ) {
        return null;
    }


    @Override
    public void updateColumnPlacementType( int adapterId, long columnId, PlacementType placementType ) {

    }


    @Override
    public void updateColumnPlacementPhysicalPosition( int adapterId, long columnId, long position ) {

    }


    @Override
    public void updateColumnPlacementPhysicalPosition( int adapterId, long columnId ) {

    }


    @Override
    public void updateColumnPlacementPhysicalNames( int adapterId, long columnId, String physicalSchemaName, String physicalTableName, String physicalColumnName, boolean updatePhysicalColumnPosition ) {

    }


    @Override
    public List<CatalogColumn> getColumns( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumn> getColumns( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) {
        return null;
    }


    @Override
    public CatalogColumn getColumn( long columnId ) {
        return null;
    }


    @Override
    public CatalogColumn getColumn( long tableId, String columnName ) throws UnknownColumnException {
        return null;
    }


    @Override
    public CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownDatabaseException, UnknownTableException {
        return null;
    }


    @Override
    public long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) {
        return 0;
    }


    @Override
    public void renameColumn( long columnId, String name ) {

    }


    @Override
    public void setColumnPosition( long columnId, int position ) {

    }


    @Override
    public void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer precision, Integer dimension, Integer cardinality ) throws GenericCatalogException {

    }


    @Override
    public void setNullable( long columnId, boolean nullable ) throws GenericCatalogException {

    }


    @Override
    public void setCollation( long columnId, Collation collation ) {

    }


    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) {
        return false;
    }


    @Override
    public void deleteColumn( long columnId ) {

    }


    @Override
    public void setDefaultValue( long columnId, PolyType type, String defaultValue ) {

    }


    @Override
    public void deleteDefaultValue( long columnId ) {

    }


    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) {
        return null;
    }


    @Override
    public boolean isPrimaryKey( long keyId ) {
        return false;
    }


    @Override
    public boolean isForeignKey( long keyId ) {
        return false;
    }


    @Override
    public boolean isIndex( long keyId ) {
        return false;
    }


    @Override
    public boolean isConstraint( long keyId ) {
        return false;
    }


    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {

    }


    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogIndex> getIndexes( CatalogKey key ) {
        return null;
    }


    @Override
    public List<CatalogIndex> getForeignKeys( CatalogKey key ) {
        return null;
    }


    @Override
    public List<CatalogConstraint> getConstraints( CatalogKey key ) {
        return null;
    }


    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) throws UnknownConstraintException {
        return null;
    }


    @Override
    public CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws UnknownForeignKeyException {
        return null;
    }


    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException {

    }


    @Override
    public void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) throws GenericCatalogException {

    }


    @Override
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) {
        return null;
    }


    @Override
    public CatalogIndex getIndex( long tableId, String indexName ) throws UnknownIndexException {
        return null;
    }


    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) {
        return false;
    }


    @Override
    public CatalogIndex getIndex( long indexId ) {
        return null;
    }


    @Override
    public List<CatalogIndex> getIndexes() {
        return null;
    }


    @Override
    public long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, int location, IndexType type, String indexName ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void setIndexPhysicalName( long indexId, String physicalName ) {

    }


    @Override
    public void deleteIndex( long indexId ) {

    }


    @Override
    public void deletePrimaryKey( long tableId ) throws GenericCatalogException {

    }


    @Override
    public void deleteForeignKey( long foreignKeyId ) throws GenericCatalogException {

    }


    @Override
    public void deleteConstraint( long constraintId ) throws GenericCatalogException {

    }


    @Override
    public CatalogUser getUser( String userName ) throws UnknownUserException {
        return null;
    }


    @Override
    public CatalogUser getUser( int userId ) {
        return null;
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        return null;
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        return null;
    }


    @Override
    public CatalogAdapter getAdapter( int adapterId ) {
        return adapters.get( adapterId );
    }


    @Override
    public boolean checkIfExistsAdapter( int adapterId ) {
        return adapters.containsKey( adapterId );
    }


    @Override
    public int addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        i++;
        adapters.put( i, new CatalogAdapter( i, uniqueName, clazz, type, settings ) );
        return i;
    }


    @Override
    public void updateAdapterSettings( int adapterId, Map<String, String> newSettings ) {

    }


    @Override
    public void deleteAdapter( int adapterId ) {

    }


    @Override
    public List<CatalogQueryInterface> getQueryInterfaces() {
        return null;
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        return null;
    }


    @Override
    public CatalogQueryInterface getQueryInterface( int ifaceId ) {
        return null;
    }


    @Override
    public int addQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        return 0;
    }


    @Override
    public void deleteQueryInterface( int ifaceId ) {

    }


    @Override
    public long addPartition( long tableId, String partitionName, long schemaId, int ownerId, PartitionType partitionType, List<String> effectivePartitionQualifier, boolean isUnbound ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void deletePartition( long tableId, long schemaId, long partitionId ) {

    }


    @Override
    public CatalogPartition getPartition( long partitionId ) {
        return null;
    }


    @Override
    public void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitions, List<Long> partitionIds ) {

    }


    @Override
    public void mergeTable( long tableId ) {

    }


    @Override
    public List<CatalogPartition> getPartitions( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitions( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<String> getPartitionNames( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByPartition( long tableId, long partitionId, long columnId ) {
        return null;
    }


    @Override
    public List<CatalogAdapter> getAdaptersByPartition( long tableId, long partitionId ) {
        return null;
    }


    @Override
    public void updatePartitionsOnDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {

    }


    @Override
    public List<Long> getPartitionsOnDataPlacement( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionsIndexOnDataPlacement( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public void deletePartitionsOnDataPlacement( int storeId, long tableId ) {

    }


    @Override
    public boolean validatePartitionDistribution( int adapterId, long tableId, long columnId ) {
        return false;
    }


    @Override
    public void flagTableForDeletion( long tableId, boolean flag ) {

    }


    @Override
    public boolean isTableFlaggedForDeletion( long tableId ) {
        return false;
    }


    @Override
    public void close() {

    }


    @Override
    public void clear() {

    }

}
