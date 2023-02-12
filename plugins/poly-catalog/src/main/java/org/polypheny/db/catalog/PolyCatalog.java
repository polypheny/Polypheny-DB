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

package org.polypheny.db.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.document.DocumentCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
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
import org.polypheny.db.catalog.graph.GraphCatalog;
import org.polypheny.db.catalog.relational.RelationalCatalog;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 * Object are as follows:
 * Namespace -> Schema (Relational), Graph (Graph), Database (Document)
 * Entity -> Table (Relational), does not exist (Graph), Collection (Document)
 * Field -> Column (Relational), does not exist (Graph), Field (Document)
 */
@Slf4j
public class PolyCatalog extends Catalog {

    private final RelationalCatalog relational;
    private final GraphCatalog graphs;
    private final DocumentCatalog documents;

    private final ImmutableList<ModelCatalog> catalogs;

    private final Map<Long, CatalogUser> users = new HashMap<>();

    private final AtomicLong namespaceIdBuilder = new AtomicLong( 0 );


    public PolyCatalog() {
        this.documents = new DocumentCatalog();
        this.graphs = new GraphCatalog();
        this.relational = new RelationalCatalog();

        catalogs = ImmutableList.of( this.relational, this.graphs, this.documents );
    }


    @Override
    public void commit() throws NoTablePrimaryKeyException {
        log.debug( "commit" );
        catalogs.stream().filter( ModelCatalog::hasUncommitedChanges ).forEach( ModelCatalog::commit );
    }


    @Override
    public void rollback() {
        log.debug( "rollback" );
        catalogs.stream().filter( ModelCatalog::hasUncommitedChanges ).forEach( ModelCatalog::rollback );
    }


    @Override
    public Map<Long, AlgDataType> getAlgTypeInfo() {
        return null;
    }


    @Override
    public Map<Long, AlgNode> getNodeInfo() {
        return null;
    }


    @Override
    public void restoreInterfacesIfNecessary() {

    }


    @Override
    public void validateColumns() {

    }


    @Override
    public void restoreColumnPlacements( Transaction transaction ) {

    }


    @Override
    public void restoreViews( Transaction transaction ) {

    }


    @Override
    public int addUser( String name, String password ) {
        return 0;
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


    // todo rename "create"
    @Override
    public long addNamespace( String name, long databaseId, int ownerId, NamespaceType namespaceType ) {
        long id = namespaceIdBuilder.getAndIncrement();
        documents.createDatabase( id, name, databaseId, namespaceType );
        graphs.createGraph( id, name, databaseId, namespaceType );
        relational.createSchema( id, name, databaseId, namespaceType );

        return id;
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
    public long addGraph( long databaseId, String name, List<DataStore> stores, boolean modifiable, boolean ifNotExists, boolean replace ) {
        return 0;
    }


    @Override
    public void addGraphLogistics( long id, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException, UnknownTableException, UnknownColumnException {

    }


    @Override
    public void deleteGraph( long id ) {

    }


    @Override
    public CatalogGraphDatabase getGraph( long id ) {
        return null;
    }


    @Override
    public List<CatalogGraphDatabase> getGraphs( long databaseId, Pattern graphName ) {
        return null;
    }


    @Override
    public void addGraphAlias( long graphId, String alias, boolean ifNotExists ) {

    }


    @Override
    public void removeGraphAlias( long graphId, String alias, boolean ifExists ) {

    }


    @Override
    public CatalogGraphMapping getGraphMapping( long graphId ) {
        return null;
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
    public CatalogTable getTableFromPartition( long partitionId ) {
        return null;
    }


    @Override
    public long addTable( String name, long namespaceId, int ownerId, EntityType entityType, boolean modifiable ) {
        return 0;
    }


    @Override
    public long addView( String name, long namespaceId, int ownerId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, String query, QueryLanguage language ) {
        return 0;
    }


    @Override
    public long addMaterializedView( String name, long namespaceId, int ownerId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public boolean checkIfExistsEntity( long namespaceId, String entityName ) {
        return false;
    }


    @Override
    public boolean checkIfExistsEntity( long tableId ) {
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
    public void addColumnPlacement( int adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) {

    }


    @Override
    public void deleteColumnPlacement( int adapterId, long columnId, boolean columnOnly ) {

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
    public List<CatalogColumnPlacement> getColumnPlacement( long columnId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterSortedByPhysicalPosition( int adapterId, long tableId ) {
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
    public ImmutableMap<Integer, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId ) {
        return null;
    }


    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getPartitionPlacementsByAdapter( long tableId ) {
        return null;
    }


    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getPartitionGroupsByAdapter( long tableId ) {
        return null;
    }


    @Override
    public long getPartitionGroupByPartition( long partitionId ) {
        return 0;
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
    public void updateColumnPlacementPhysicalNames( int adapterId, long columnId, String physicalSchemaName, String physicalColumnName, boolean updatePhysicalColumnPosition ) {

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
        return null;
    }


    @Override
    public boolean checkIfExistsAdapter( int adapterId ) {
        return false;
    }


    @Override
    public int addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        return 0;
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
    public long addPartitionGroup( long tableId, String partitionGroupName, long schemaId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId ) {

    }


    @Override
    public CatalogPartitionGroup getPartitionGroup( long partitionGroupId ) {
        return null;
    }


    @Override
    public long addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
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
    public List<CatalogPartition> getPartitionsByTable( long tableId ) {
        return null;
    }


    @Override
    public void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitionGroups, List<Long> partitionGroupIds, PartitionProperty partitionProperty ) {

    }


    @Override
    public void mergeTable( long tableId ) {

    }


    @Override
    public void updateTablePartitionProperties( long tableId, PartitionProperty partitionProperty ) {

    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public void updatePartitionGroup( long partitionGroupId, List<Long> partitionIds ) {

    }


    @Override
    public void addPartitionToGroup( long partitionGroupId, Long partitionId ) {

    }


    @Override
    public void removePartitionFromGroup( long partitionGroupId, Long partitionId ) {

    }


    @Override
    public void updatePartition( long partitionId, Long partitionGroupId ) {

    }


    @Override
    public List<CatalogPartition> getPartitions( long partitionGroupId ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitions( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<String> getPartitionGroupNames( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId ) {
        return null;
    }


    @Override
    public List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionGroupsOnDataPlacement( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionsOnDataPlacement( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionGroupsIndexOnDataPlacement( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public CatalogDataPlacement getDataPlacement( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByRole( long tableId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByIdAndRole( long tableId, long partitionId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved ) {
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
    public void addPartitionPlacement( int adapterId, long tableId, long partitionId, PlacementType placementType, String physicalSchemaName, String physicalTableName, DataPlacementRole role ) {

    }


    @Override
    public void addDataPlacement( int adapterId, long tableId ) {

    }


    @Override
    public CatalogDataPlacement addDataPlacementIfNotExists( int adapterId, long tableId ) {
        return null;
    }


    @Override
    protected void modifyDataPlacement( int adapterId, long tableId, CatalogDataPlacement catalogDataPlacement ) {

    }


    @Override
    public long addGraphPlacement( int adapterId, long graphId ) {
        return 0;
    }


    @Override
    public List<CatalogGraphPlacement> getGraphPlacements( int adapterId ) {
        return null;
    }


    @Override
    public void deleteGraphPlacement( int adapterId, long graphId ) {

    }


    @Override
    public void updateGraphPlacementPhysicalNames( long graphId, int adapterId, String physicalGraphName ) {

    }


    @Override
    public CatalogGraphPlacement getGraphPlacement( long graphId, int adapterId ) {
        return null;
    }


    @Override
    public void removeDataPlacement( int adapterId, long tableId ) {

    }


    @Override
    protected void addSingleDataPlacementToTable( Integer adapterId, long tableId ) {

    }


    @Override
    protected void removeSingleDataPlacementFromTable( Integer adapterId, long tableId ) {

    }


    @Override
    public void updateDataPlacementsOnTable( long tableId, List<Integer> newDataPlacements ) {

    }


    @Override
    protected void addColumnsToDataPlacement( int adapterId, long tableId, List<Long> columnIds ) {

    }


    @Override
    protected void removeColumnsFromDataPlacement( int adapterId, long tableId, List<Long> columnIds ) {

    }


    @Override
    protected void addPartitionsToDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {

    }


    @Override
    protected void removePartitionsFromDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {

    }


    @Override
    public void updateDataPlacement( int adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds ) {

    }


    @Override
    public void updatePartitionPlacementPhysicalNames( int adapterId, long partitionId, String physicalSchemaName, String physicalTableName ) {

    }


    @Override
    public void deletePartitionPlacement( int adapterId, long partitionId ) {

    }


    @Override
    public CatalogPartitionPlacement getPartitionPlacement( int adapterId, long partitionId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( int adapterId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( int adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId ) {
        return null;
    }


    @Override
    public List<CatalogTable> getTablesForPeriodicProcessing() {
        return null;
    }


    @Override
    public void addTableToPeriodicProcessing( long tableId ) {

    }


    @Override
    public void removeTableFromPeriodicProcessing( long tableId ) {

    }


    @Override
    public boolean checkIfExistsPartitionPlacement( int adapterId, long partitionId ) {
        return false;
    }


    @Override
    public void deleteViewDependencies( CatalogView catalogView ) {

    }


    @Override
    public void updateMaterializedViewRefreshTime( long materializedViewId ) {

    }


    @Override
    public CatalogCollection getCollection( long collectionId ) {
        return null;
    }


    @Override
    public List<CatalogCollection> getCollections( long namespaceId, Pattern namePattern ) {
        return null;
    }


    @Override
    public long addCollection( Long id, String name, long schemaId, int currentUserId, EntityType entity, boolean modifiable ) {
        return 0;
    }


    @Override
    public long addCollectionPlacement( int adapterId, long collectionId, PlacementType placementType ) {
        return 0;
    }


    @Override
    public CatalogCollectionMapping getCollectionMapping( long id ) {
        return null;
    }


    @Override
    public long addCollectionLogistics( long schemaId, String name, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( int adapterId ) {
        return null;
    }


    @Override
    public CatalogCollectionPlacement getCollectionPlacement( long collectionId, int adapterId ) {
        return null;
    }


    @Override
    public void updateCollectionPartitionPhysicalNames( long collectionId, int adapterId, String physicalNamespaceName, String namespaceName, String physicalCollectionName ) {

    }


    @Override
    public void deleteCollection( long id ) {

    }


    @Override
    public void dropCollectionPlacement( long id, int adapterId ) {

    }


    @Override
    public void close() {

    }


    @Override
    public void clear() {

    }

}
