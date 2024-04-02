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


import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.partition.raw.RawPartitionInformation;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Abstract class for the DDLManager, goal of this class is to expose a unified interface,
 * which allows to handle DDLs. Especially with regard to different models.
 * The ddl methods should contain all logic needed for them and throw appropriate exceptions to the callee.
 */
public abstract class DdlManager {

    public static DdlManager INSTANCE = null;


    public static final List<String> blockedNamespaceNames = List.of( "namespace", "db", "schema", "graph", "database" );


    /**
     * Sets a new DdlManager and returns it.
     *
     * @param manager the DdlManager which is set
     * @return the instance of the DdlManager, which has been set
     */
    public static DdlManager setAndGetInstance( DdlManager manager ) {
        if ( INSTANCE != null ) {
            throw new GenericRuntimeException( "Overwriting the DdlManger, when already set is not permitted." );
        }
        INSTANCE = manager;
        return INSTANCE;
    }


    /**
     * Access Pattern for DdlManager Singleton.
     *
     * @return The DdlManager
     */
    public static DdlManager getInstance() {
        if ( INSTANCE == null ) {
            throw new GenericRuntimeException( "DdlManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    /**
     * Creates a namespace with the provided options.
     *
     * @param name name of the new namespace
     * @param type the namespace type, RELATIONAL, DOCUMENT, etc.
     * @param ifNotExists whether to silently ignore if a namespace with this name does already exist
     * @param replace whether to replace an existing namespace with this name
     * @param statement the query statement
     */
    public abstract long createNamespace( String name, DataModel type, boolean ifNotExists, boolean replace, Statement statement );

    /**
     * Adds a new data store(adapter)
     *
     * @param uniqueName unique name of the newly created store
     * @param adapterName name of store, which is used to create the store
     * @param adapterType the specific {@link AdapterType} for the store to create
     * @param config configuration for the store
     * @param mode the deploy mode
     */
    public abstract void createStore( String uniqueName, String adapterName, AdapterType adapterType, Map<String, String> config, DeployMode mode );


    /**
     * Adds a new data source(adapter)
     *
     * @param uniqueName unique name of the newly created source
     * @param adapterName name of source, which is used to create the source
     * @param namespace the target namespace for the adapter
     * @param adapterType the specific {@link AdapterType} for the source to create
     * @param config configuration for the source
     * @param mode the deploy mode
     */
    public abstract void createSource( String uniqueName, String adapterName, long namespace, AdapterType adapterType, Map<String, String> config, DeployMode mode );


    /**
     * Drop an adapter
     *
     * @param name name of the adapter to be dropped
     * @param statement the query statement
     */
    public abstract void dropAdapter( String name, Statement statement );

    /**
     * Change the name of a namespace
     *
     * @param newName the new name for the namespace
     * @param currentName the current name of the namespace
     */
    public abstract void renameNamespace( String newName, String currentName );

    /**
     * Adds a column to an existing source table
     *
     * @param table the table
     * @param columnPhysicalName the physical name of the new column
     * @param columnLogicalName the name of the new column
     * @param beforeColumnName the name of the column before the column which is inserted; can be null
     * @param afterColumnName the name of the column after the column, which is inserted; can be null
     * @param defaultValue the default value of the inserted column
     */
    public abstract void addColumnToSourceTable( LogicalTable table, String columnPhysicalName, String columnLogicalName, String beforeColumnName, String afterColumnName, PolyValue defaultValue, Statement statement );

    /**
     * Add a column to an existing table
     *
     * @param columnName the name of the new column
     * @param table the table
     * @param beforeColumnName the column before which the new column should be positioned; can be null
     * @param afterColumnName the column after which the new column should be positioned; can be null
     * @param type the SQL data type specification of the new column
     * @param nullable if the column can hold the value NULL
     * @param defaultValue a default value for the column; can be null
     * @param statement the query statement
     */
    public abstract void createColumn( String columnName, LogicalTable table, String beforeColumnName, String afterColumnName, ColumnTypeInformation type, boolean nullable, PolyValue defaultValue, Statement statement );

    /**
     * Add a foreign key to a table
     *
     * @param table the table
     * @param refTable the table being referenced
     * @param columnNames the names of the columns
     * @param refColumnNames the names of the columns which are referenced
     * @param constraintName the name of this new foreign key constraint
     * @param onUpdate how to enforce the constraint on updated
     * @param onDelete how to enforce the constraint on delete
     */
    public abstract void createForeignKey( LogicalTable table, LogicalTable refTable, List<String> columnNames, List<String> refColumnNames, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete );

    /**
     * Adds an index to a table
     *
     * @param table the table to which an index should be added
     * @param indexMethodName name of the indexMethod; can be null
     * @param columnNames logical names of all columns on which to create the index
     * @param indexName name of the index
     * @param isUnique whether the index is unique
     * @param location instance of the data store on which to create the index; if null, default strategy is being used
     * @param statement the initial query statement
     */
    public abstract void createIndex( LogicalTable table, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, DataStore<?> location, Statement statement ) throws TransactionException;

    /**
     * Adds an index located in Polypheny to a table
     *
     * @param table the table to which an index should be added
     * @param indexMethodName name of the indexMethod; can be null
     * @param columnNames logical names of all columns on which to create the index
     * @param indexName name of the index
     * @param isUnique whether the index is unique
     * @param statement the initial query statement
     */
    public abstract void createPolyphenyIndex( LogicalTable table, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, Statement statement ) throws TransactionException;

    /**
     * Adds new column placements to a table
     *
     * @param table the table
     * @param columnIds the ids of the columns for which to create a new placement
     * @param partitionGroupIds the ids of the partitions of the column
     * @param partitionGroupNames the name for these partition
     * @param dataStore the data store on which to create the placement
     * @param statement the query statement
     */
    public abstract void createAllocationPlacement( LogicalTable table, List<LogicalColumn> columnIds, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore<?> dataStore, Statement statement );

    /**
     * Adds a new primary key to a table
     *
     * @param table the table
     * @param columnNames the names of all columns in the primary key
     * @param statement the query statement
     */
    public abstract void createPrimaryKey( LogicalTable table, List<String> columnNames, Statement statement );

    /**
     * Adds a unique constraint to a table
     *
     * @param table the target table
     * @param columnNames the names of the columns which are part of the constraint
     * @param constraintName the name of the unique constraint
     */
    public abstract void createUniqueConstraint( LogicalTable table, List<String> columnNames, String constraintName );

    /**
     * Drop a specific column in a table
     *
     * @param table the table
     * @param columnName the name of column which is dropped
     * @param statement the query statement
     */
    public abstract void dropColumn( LogicalTable table, String columnName, Statement statement );

    /**
     * Drop a specific constraint from a table
     *
     * @param table the table
     * @param constraintName the name of the constraint to be dropped
     */
    public abstract void dropConstraint( LogicalTable table, String constraintName );

    /**
     * Drop a foreign key of a table
     *
     * @param table the table the foreign key belongs to
     * @param foreignKeyName the name of the foreign key to drop
     */
    public abstract void dropForeignKey( LogicalTable table, String foreignKeyName );

    /**
     * Drop an indexes
     *
     * @param table the table the index belongs to
     * @param indexName the name of the index to drop
     * @param statement the query statement
     */
    public abstract void dropIndex( LogicalTable table, String indexName, Statement statement );

    /**
     * Drop the data placement of a table on a specified data store
     *
     * @param table the table for which to drop a placement
     * @param store the data store from which to drop the placement
     * @param statement the query statement
     */
    public abstract void dropPlacement( LogicalTable table, DataStore<?> store, Statement statement );

    /**
     * Drop the primary key of a table
     *
     * @param table the table
     */
    public abstract void dropPrimaryKey( LogicalTable table );

    /**
     * Set the type of the column
     *
     * @param table the table
     * @param columnName the name of the column to be modified
     * @param typeInformation the new type of the column
     * @param statement the used statement
     */
    public abstract void setColumnType( LogicalTable table, String columnName, ColumnTypeInformation typeInformation, Statement statement );

    /**
     * Set if the column can hold the value NULL or not
     *
     * @param table the table
     * @param columnName the name of the column to be modified
     * @param nullable if the column should be nullable
     * @param statement the used statement
     */
    public abstract void setColumnNullable( LogicalTable table, String columnName, boolean nullable, Statement statement );

    /**
     * Changes the position of the column and places it before or after the provided columns
     *
     * @param table the table
     * @param columnName the name of the column to be modified
     * @param beforeColumnName change position of the column and place it before this column; nullable
     * @param afterColumnName change position of the column and place it after this column; nullable
     * @param statement the used statement
     */
    public abstract void setColumnPosition( LogicalTable table, String columnName, String beforeColumnName, String afterColumnName, Statement statement );

    /**
     * Set the collation to the column
     *
     * @param table the table
     * @param columnName the name of the column to be modified
     * @param collation the new collation of the column
     * @param statement the used statement
     */
    public abstract void setColumnCollation( LogicalTable table, String columnName, Collation collation, Statement statement );

    /**
     * Set the default value of the column
     *
     * @param table the table
     * @param columnName the name of the column to be modified
     * @param defaultValue the new default value of the column
     * @param statement the used statement
     */
    public abstract void setDefaultValue( LogicalTable table, String columnName, PolyValue defaultValue, Statement statement );

    /**
     * Drop the default value of the column
     *
     * @param table the table
     * @param columnName the name of the column to be modified
     * @param statement the used statement
     */
    public abstract void dropDefaultValue( LogicalTable table, String columnName, Statement statement );

    /**
     * Modify the placement of a table on a specified data store. This method compares the specified list of column ids with
     * the currently placed columns. If a column currently present on the data store is not specified in the columnIds list,
     * the column placement is removed. In case the column to be removed is part of the primary key, it is not removed but the
     * placement type is changed to automatic. Vise versa, for columns specified in the list which are not yet placed on the
     * data store a column placement is created. In case there is already a column placement of type automatic, the type is
     * changed to manual.
     *
     * @param table the table
     * @param columns which columns should be placed on the specified data store
     * @param partitionGroupIds the ids of the partitions of this column
     * @param partitionGroupNames the name of these partitions
     * @param storeInstance the data store
     * @param statement the used statement
     */
    public abstract void modifyPlacement( LogicalTable table, List<Long> columns, List<Integer> partitionGroupIds, List<String> partitionGroupNames, DataStore<?> storeInstance, Statement statement );

    /**
     * Modified the partition distribution on the selected store. Can be used to add or remove partitions on a store.
     * Which consequently alters the Partition Placements.
     *
     * @param table the table
     * @param partitionGroupIds the desired target state of partition groups which should remain on this store
     * @param store the data store on which the partition placements should be altered
     * @param statement the used statement
     */
    public abstract void modifyPartitionPlacement( LogicalTable table, List<Long> partitionGroupIds, DataStore<?> store, Statement statement );

    /**
     * Add a column placement for a specified column on a specified data store. If the store already contains a placement of
     * the column with type automatic, the placement type is changed to manual.
     *
     * @param table the table
     * @param column the column name for which to add a placement
     * @param store the data store on which the column should be placed
     * @param statement the used statement
     */
    public abstract void createColumnPlacement( LogicalTable table, LogicalColumn column, DataStore<?> store, Statement statement );

    /**
     * Drop a specified column from a specified data store. If the column is part of the primary key, the column placement typ
     * is changed to automatic.
     *
     * @param table the table
     * @param column the name of the column for which to drop a placement
     * @param store the data store from which to remove the placement
     * @param statement the used statement
     */
    public abstract void dropColumnPlacement( LogicalTable table, LogicalColumn column, DataStore<?> store, Statement statement );

    /**
     * Rename a table (changing the logical name of the table)
     *
     * @param table the table to be renamed
     * @param newTableName the new name for the table
     * @param statement the used statement
     */
    public abstract void renameTable( LogicalTable table, String newTableName, Statement statement );

    public abstract void renameCollection( LogicalCollection collection, String newName, Statement statement );

    /**
     * Rename a column of a table (changing the logical name of the column)
     *
     * @param table the table in which the column resides
     * @param columnName the old name of the column to be renamed
     * @param newColumnName the new name for the column
     * @param statement the used statement
     */
    public abstract void renameColumn( LogicalTable table, String columnName, String newColumnName, Statement statement );

    public abstract void dropGraph( long graphId, boolean ifExists, Statement statement );

    /**
     * Create a new table
     *
     * @param namespaceId the id of the schema to which the table belongs
     * @param tableName the name of the new table
     * @param columns all columns of the table
     * @param constraints all constraints for the table
     * @param ifNotExists whether to silently ignore if the table already exists
     * @param stores list of data stores on which to create a full placement for this table
     * @param placementType which placement type should be used for the initial placements
     * @param statement the used statement
     */
    public abstract void createTable( long namespaceId, String tableName, List<FieldInformation> columns, List<ConstraintInformation> constraints, boolean ifNotExists, @Nullable List<DataStore<?>> stores, PlacementType placementType, Statement statement );

    /**
     * Create a new view
     *
     * @param viewName the name of the new view
     * @param namespaceId the id of the schema to which the view belongs
     * @param algNode the algNode which was built form the Select part of the view
     * @param statement the used Statement
     */
    public abstract void createView( String viewName, long namespaceId, AlgNode algNode, AlgCollation algCollation, boolean replace, Statement statement, PlacementType placementType, List<String> projectedColumns, String query, QueryLanguage language );


    /**
     * Create a new materialized view
     *
     * @param viewName the name of the new view
     * @param namespaceId the id of the schema to which the view belongs
     * @param algRoot the relNode which was built form the Select part of the view
     * @param statement the used Statement
     */
    public abstract void createMaterializedView( String viewName, long namespaceId, AlgRoot algRoot, boolean replace, Statement statement, List<DataStore<?>> stores, PlacementType placementType, List<String> projectedColumns, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ifNotExists, boolean ordered );

    public abstract void createCollection( long namespaceId, String name, boolean ifNotExists, List<DataStore<?>> stores, PlacementType placementType, Statement statement );

    public abstract void createCollectionPlacement( long namespaceId, String name, List<DataStore<?>> stores, Statement statement );

    /**
     * Add new partitions for the column
     *
     * @param partitionInfo the information concerning the partition
     */
    public abstract void createTablePartition( PartitionInformation partitionInfo, List<DataStore<?>> stores, Statement statement ) throws TransactionException;

    /**
     * Removes partitioning from Table
     *
     * @param table teh table to be merged
     * @param statement the used Statement
     */
    public abstract void dropTablePartition( LogicalTable table, Statement statement ) throws TransactionException;

    /**
     * Adds a new constraint to a table
     */
    public abstract void createConstraint( ConstraintInformation information, long namespaceId, List<Long> columnIds, long tableId );

    /**
     * Drop a NAMESPACE
     *
     * @param namespaceName the name of the namespace to drop
     * @param ifExists whether to silently ignore if there is no namespace with this name
     * @param statement the used statement
     */
    public abstract void dropNamespace( String namespaceName, boolean ifExists, Statement statement );

    /**
     * Drop a table
     *
     * @param table the table to be dropped
     * @param statement the used statement
     */
    public abstract void dropTable( LogicalTable table, Statement statement );

    /**
     * Drop View
     */
    public abstract void dropView( LogicalTable view, Statement statement );


    /**
     * @param materializedView to be dropped
     * @param statement the used statement
     */
    public abstract void dropMaterializedView( LogicalTable materializedView, Statement statement );

    /**
     * Truncate a table
     *
     * @param table the table to be truncated
     * @param statement the used statement
     */
    public abstract void truncate( LogicalTable table, Statement statement );

    /**
     * Create a new type
     */
    public abstract void createType();

    /**
     * Drop a type
     */
    public abstract void dropType();

    /**
     * Drop a function
     */
    public abstract void dropFunction();

    /**
     * Set an option
     */
    public abstract void setOption();

    /**
     * Refresh data in a Materialized View
     */
    public abstract void refreshView( Statement statement, Long materializedId );

    public abstract long createGraph( String namespaceName, boolean modifiable, @Nullable List<DataStore<?>> stores, boolean ifNotExists, boolean replace, boolean caseSensitive, Statement statement );

    public abstract void createGraphAlias( long graphId, String alias, boolean ifNotExists );

    public abstract void dropGraphAlias( long graphId, String alias, boolean ifNotExists );


    public abstract void replaceGraphAlias( long graphId, String oldAlias, String alias );


    public abstract long createGraphPlacement( long graphId, List<DataStore<?>> stores, Statement statement );

    public abstract void dropGraphPlacement( long graphId, DataStore<?> dataStores, Statement statement );


    public abstract void dropCollection( LogicalCollection catalogCollection, Statement statement );

    public abstract void dropCollectionPlacement( long namespaceId, LogicalCollection collection, List<DataStore<?>> dataStores, Statement statement );


    /**
     * Helper class which holds all information required for creating a column,
     * decoupled from a specific query language
     */
    @Value
    public static class FieldInformation {

        public String name;
        public ColumnTypeInformation typeInformation;
        public Collation collation;
        public PolyValue defaultValue;
        public int position;


        public FieldInformation( String name, ColumnTypeInformation typeInformation, Collation collation, PolyValue defaultValue, int position ) {
            this.name = name;
            this.typeInformation = typeInformation;
            this.collation = collation;
            this.defaultValue = defaultValue;
            this.position = position;
        }

    }


    /**
     * Helper class which holds all information required for creating a constraint,
     * decoupled from its query language
     */

    @Value
    public static class ConstraintInformation {

        public String name;
        public ConstraintType type;
        public List<String> columnNames;
        public @Nullable String foreignKeyTable;
        public @Nullable String foreignKeyColumnName;


        public ConstraintInformation( String name, ConstraintType type, List<String> columnNames, @Nullable String foreignKeyTable, @Nullable String foreignKeyColumnName ) {
            this.name = name;
            this.type = type;
            this.columnNames = columnNames;
            this.foreignKeyTable = foreignKeyTable;
            this.foreignKeyColumnName = foreignKeyColumnName;
        }


        public ConstraintInformation( String name, ConstraintType type, List<String> columnNames ) {
            this( name, type, columnNames, null, null );
        }

    }


    /**
     * Helper class, which holds all type information for a column
     * decoupled from the used query language
     */
    @Value
    public static class ColumnTypeInformation {

        public PolyType type;
        @Nullable
        public PolyType collectionType;
        public Integer precision;
        public Integer scale;
        public Integer dimension;
        public Integer cardinality;
        public Boolean nullable;


        public ColumnTypeInformation(
                PolyType type,
                @Nullable PolyType collectionType,
                Integer precision,
                Integer scale,
                Integer dimension,
                Integer cardinality,
                Boolean nullable ) {
            this.type = type;
            this.collectionType = collectionType == type ? null : collectionType;
            this.precision = precision == null || precision == -1 ? null : precision;
            this.scale = scale == null || scale == -1 || scale == Integer.MIN_VALUE ? null : scale;
            this.dimension = dimension == null || dimension == -1 ? null : dimension;
            this.cardinality = cardinality == null || cardinality == -1 ? null : cardinality;
            this.nullable = nullable;
        }


        public static ColumnTypeInformation fromDataTypeSpec( DataTypeSpec sqlDataType ) {
            return new ColumnTypeInformation(
                    sqlDataType.getType(),
                    sqlDataType.getCollectionsType(),
                    sqlDataType.getPrecision(),
                    sqlDataType.getScale(),
                    sqlDataType.getDimension(),
                    sqlDataType.getCardinality(),
                    sqlDataType.getNullable() );
        }

    }


    @Value
    @SuperBuilder(toBuilder = true)
    public static class PartitionInformation {

        public LogicalTable table;
        public String columnName;
        public String typeName;
        public List<String> partitionGroupNames;
        public int numberOfPartitionGroups;
        public int numberOfPartitions;
        public List<List<String>> qualifiers;
        public RawPartitionInformation rawPartitionInformation;


        public PartitionInformation(
                LogicalTable table,
                String typeName,
                String columnName,
                List<String> partitionGroupNames,
                int numberOfPartitionGroups,
                int numberOfPartitions,
                List<List<String>> qualifiers,
                RawPartitionInformation rawPartitionInformation ) {
            this.table = table;
            this.typeName = typeName;
            this.columnName = columnName;
            this.partitionGroupNames = partitionGroupNames;
            this.numberOfPartitionGroups = numberOfPartitionGroups;
            this.numberOfPartitions = numberOfPartitions;
            this.qualifiers = qualifiers;
            this.rawPartitionInformation = rawPartitionInformation;
        }


        public static PartitionInformation fromNodeLists(
                LogicalTable table,
                String typeName,
                String columnName,
                List<Identifier> partitionGroupNames,
                int numberOfPartitionGroups,
                int numberOfPartitions,
                List<List<Node>> partitionQualifierList,
                RawPartitionInformation rawPartitionInformation ) {
            List<String> names = partitionGroupNames
                    .stream()
                    .map( Identifier::getSimple )
                    .toList();
            List<List<String>> qualifiers = partitionQualifierList
                    .stream()
                    .map( qs -> qs.stream().map( PartitionInformation::getValueOfSqlNode ).toList() )
                    .toList();
            return new PartitionInformation( table, typeName, columnName, names, numberOfPartitionGroups, numberOfPartitions, qualifiers, rawPartitionInformation );
        }


        /**
         * Needed to modify strings otherwise the SQL-input 'a' will be also added as the value "'a'" and not as "a" as intended
         * Essentially removes " ' " at the start and end of value
         *
         * @param node Node to be modified
         * @return String
         */
        public static String getValueOfSqlNode( Node node ) {

            if ( node instanceof Literal ) {
                return ((Literal) node).toValue();
            }
            return node.toString();
        }

    }


    public enum DefaultIndexPlacementStrategy {
        POLYPHENY, ONE_DATA_STORE, ALL_DATA_STORES
    }

}
