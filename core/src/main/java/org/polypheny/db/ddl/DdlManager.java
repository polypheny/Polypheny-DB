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


import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.exception.AlterSourceException;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.IndexExistsException;
import org.polypheny.db.ddl.exception.IndexPreventsRemovalException;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.MissingColumnPlacementException;
import org.polypheny.db.ddl.exception.NotNullAndDefaultValueException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementIsPrimaryException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.ddl.exception.UnknownIndexMethodException;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;

/**
 * Abstract class for the DDLManager, goal of this class is to expose a unified interface,
 * which allows to handle DDLs. Especially with regard to different models.
 * The ddl methods should contain all logic needed for them and throw appropriate exceptions to the callee.
 */
public abstract class DdlManager {

    public static DdlManager INSTANCE = null;


    public enum Language {
        SQL( 1 ),
        MQL( 2 );

        private final int id;


        Language( int id ) {
            this.id = id;
        }
    }


    /**
     * Sets a new DdlManager and returns it.
     *
     * @param manager the DdlManager which is set
     * @return the instance of the DdlManager, which has been set
     */
    public static DdlManager setAndGetInstance( DdlManager manager ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the DdlManger, when already set is not permitted." );
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
            throw new RuntimeException( "DdlManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    /**
     * Creates a schema with the provided options.
     *
     * @param name name of the new schema
     * @param databaseId id of the database, to which the schema belongs
     * @param type the schema type, RELATIONAL, DOCUMENT, etc.
     * @param userId the owner of the new schema
     * @param ifNotExists whether to silently ignore if the schema does already exist
     * @param replace whether the replace a existing schema
     */
    public abstract void createSchema( String name, long databaseId, SchemaType type, int userId, boolean ifNotExists, boolean replace ) throws SchemaAlreadyExistsException;

    /**
     * Adds a new adapter (data store or data source)
     *
     * @param adapterName unique name of the newly created adapter
     * @param clazzName class to be used for creating the adapter instance
     * @param config configuration for the adapter
     */
    public abstract void addAdapter( String adapterName, String clazzName, Map<String, String> config );

    /**
     * Drop an adapter
     *
     * @param name name of the adapter to be dropped
     * @param statement the query statement
     */
    public abstract void dropAdapter( String name, Statement statement ) throws UnknownAdapterException;

    /**
     * Change the owner of a schema
     *
     * @param schemaName the name of the schema for which to change the owner
     * @param ownerName the name of the new owner
     * @param databaseId the id of the database
     */
    public abstract void alterSchemaOwner( String schemaName, String ownerName, long databaseId ) throws UnknownUserException, UnknownSchemaException;

    /**
     * Change the name of a schema
     *
     * @param newName the new name for the schema
     * @param oldName the old name current name of the schema
     * @param databaseId the id of the database the schema belongs to
     */
    public abstract void renameSchema( String newName, String oldName, long databaseId ) throws SchemaAlreadyExistsException, UnknownSchemaException;

    /**
     * Adds a column to an existing source table
     *
     * @param catalogTable the table
     * @param columnPhysicalName the physical name of the new column
     * @param columnLogicalName the name of the new column
     * @param beforeColumn the column before the column which is inserted; can be null
     * @param afterColumn the column after the column, which is inserted; can be null
     * @param defaultValue the default value of the inserted column
     */
    public abstract void addColumnToSourceTable( CatalogTable catalogTable, String columnPhysicalName, String columnLogicalName, CatalogColumn beforeColumn, CatalogColumn afterColumn, String defaultValue, Statement statement ) throws ColumnAlreadyExistsException, DdlOnSourceException;

    /**
     * Add a column to an existing table
     *
     * @param columnName the name of the new column
     * @param catalogTable the table
     * @param beforeColumn the column before which the new column should be positioned; can be null
     * @param afterColumn the column after which the new column should be positioned; can be null
     * @param type the SQL data type specification of the new column
     * @param nullable if the column can hold the value NULL
     * @param defaultValue a default value for the column; can be null
     * @param statement the query statement
     */
    public abstract void addColumn( String columnName, CatalogTable catalogTable, CatalogColumn beforeColumn, CatalogColumn afterColumn, ColumnTypeInformation type, boolean nullable, String defaultValue, Statement statement ) throws NotNullAndDefaultValueException, ColumnAlreadyExistsException;

    /**
     * Add a foreign key to a table
     *
     * @param catalogTable the table
     * @param refTable the table being referenced
     * @param columnNames the names of the columns
     * @param refColumnNames the names of the columns which are referenced
     * @param constraintName the name of this new foreign key constraint
     * @param onUpdate how to enforce the constraint on updated
     * @param onDelete how to enforce the constraint on delete
     */
    public abstract void addForeignKey( CatalogTable catalogTable, CatalogTable refTable, List<String> columnNames, List<String> refColumnNames, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws UnknownColumnException, GenericCatalogException;

    /**
     * Adds an index to a table
     *
     * @param catalogTable the table to which an index should be added
     * @param indexMethodName name of the indexMethod; can be null
     * @param columnNames logical names of all columns on which to create the index
     * @param indexName name of the index
     * @param isUnique whether the index is unique
     * @param location instance of the data store on which to create the index; null for creating a polystore index
     * @param statement the initial query statement
     */
    public abstract void addIndex( CatalogTable catalogTable, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, DataStore location, Statement statement ) throws UnknownColumnException, UnknownIndexMethodException, GenericCatalogException, UnknownTableException, UnknownUserException, UnknownSchemaException, UnknownKeyException, UnknownDatabaseException, TransactionException, AlterSourceException, IndexExistsException, MissingColumnPlacementException;

    /**
     * Adds new column placements to a table
     *
     * @param catalogTable the table
     * @param columnIds the ids of the columns for which to create a new placement
     * @param dataStore the data store on which to create the placement
     * @param statement the query statement
     */
    public abstract void addPlacement( CatalogTable catalogTable, List<Long> columnIds, DataStore dataStore, Statement statement ) throws PlacementAlreadyExistsException;

    /**
     * Adds a new primary key to a table
     *
     * @param catalogTable the table
     * @param columnNames the names of all columns in the primary key
     * @param statement the query statement
     */
    public abstract void addPrimaryKey( CatalogTable catalogTable, List<String> columnNames, Statement statement ) throws DdlOnSourceException;

    /**
     * Adds a unique constraint to a table
     *
     * @param catalogTable the target table
     * @param columnNames the names of the columns which are part of the constraint
     * @param constraintName the name of the unique constraint
     */
    public abstract void addUniqueConstraint( CatalogTable catalogTable, List<String> columnNames, String constraintName ) throws DdlOnSourceException;

    /**
     * Drop a specific column in a table
     *
     * @param catalogTable the table
     * @param catalogColumn the column which is dropped
     * @param statement the query statement
     */
    public abstract void dropColumn( CatalogTable catalogTable, CatalogColumn catalogColumn, Statement statement );

    /**
     * Drop a specific constraint from a table
     *
     * @param catalogTable the table
     * @param constraintName the name of the constraint to be dropped
     */
    public abstract void dropConstraint( CatalogTable catalogTable, String constraintName ) throws DdlOnSourceException;

    /**
     * Drop a foreign key of a table
     *
     * @param catalogTable the table the foreign key belongs to
     * @param foreignKeyName the name of the foreign key to drop
     */
    public abstract void dropForeignKey( CatalogTable catalogTable, String foreignKeyName ) throws DdlOnSourceException;

    /**
     * Drop an indexes
     *
     * @param catalogTable the table the index belongs to
     * @param indexName the name of the index to drop
     * @param statement the query statement
     */
    public abstract void dropIndex( CatalogTable catalogTable, String indexName, Statement statement ) throws DdlOnSourceException;

    /**
     * Drop the placement of a table on a specified data store
     *
     * @param catalogTable the table for which to to drop a placement
     * @param storeInstance the data store from which to drop the placement
     * @param statement the query statement
     */
    public abstract void dropPlacement( CatalogTable catalogTable, DataStore storeInstance, Statement statement ) throws PlacementNotExistsException, LastPlacementException;

    /**
     * Drop the primary key of a table
     *
     * @param catalogTable the table
     */
    public abstract void dropPrimaryKey( CatalogTable catalogTable ) throws DdlOnSourceException;

    /**
     * Modify a column
     *
     * @param catalogTable the table
     * @param catalogColumn the column to be modified
     * @param type the new type of the column
     * @param collation the new collation of the column
     * @param defaultValue the new default value of the column
     * @param nullable if the column should be nullable
     * @param dropDefault whether to drop the current default value
     * @param beforeColumn change position of the column and place it before this column
     * @param afterColumn change position of the column and place it after this column
     * @param statement the used statement
     */
    public abstract void alterTableModifyColumn( CatalogTable catalogTable, CatalogColumn catalogColumn, ColumnTypeInformation type, Collation collation, String defaultValue, Boolean nullable, Boolean dropDefault, CatalogColumn beforeColumn, CatalogColumn afterColumn, Statement statement ) throws DdlOnSourceException;

    /**
     * Modify the placement of a table on a specified data store. This method compares the specified list of column ids with
     * the currently placed columns. If a column currently present on the data store is not specified in the columnIds list,
     * the column placement is removed. In case the column to be removed is part of the primary key, it is not removed but the
     * placement type is changed to automatic. Vise versa, for columns specified in the list which are not yet placed on the
     * data store a column placement is created. In case there is already a column placement of type automatic, the type is
     * changed to manual.
     *
     * @param catalogTable the table
     * @param columnIds which columns should be placed on the specified data store
     * @param storeInstance the data store
     * @param statement the used statement
     */
    public abstract void modifyColumnPlacement( CatalogTable catalogTable, List<Long> columnIds, DataStore storeInstance, Statement statement ) throws PlacementNotExistsException, IndexPreventsRemovalException, LastPlacementException;

    /**
     * Add a column placement for a specified column on a specified data store. If the store already contains a placement of
     * the column with type automatic, the placement type is changed to manual.
     *
     * @param catalogTable the table
     * @param catalogColumn the column for which to add a placement
     * @param storeInstance the data store on which the column should be placed
     * @param statement the used statement
     */
    public abstract void addColumnPlacement( CatalogTable catalogTable, CatalogColumn catalogColumn, DataStore storeInstance, Statement statement ) throws UnknownAdapterException, PlacementNotExistsException, PlacementAlreadyExistsException;

    /**
     * Drop a specified column from a specified data store. If the column is part of the primary key, the column placement typ
     * is changed to automatic.
     *
     * @param catalogTable the table
     * @param catalogColumn the column for which to drop a placement
     * @param storeInstance the data store from which to remove the placement
     * @param statement the used statement
     */
    public abstract void dropColumnPlacement( CatalogTable catalogTable, CatalogColumn catalogColumn, DataStore storeInstance, Statement statement ) throws UnknownAdapterException, PlacementNotExistsException, IndexPreventsRemovalException, LastPlacementException, PlacementIsPrimaryException;

    /**
     * Change the owner of a table
     *
     * @param catalogTable the table
     * @param newOwnerName the name of the new owner
     */
    public abstract void alterTableOwner( CatalogTable catalogTable, String newOwnerName ) throws UnknownUserException;

    /**
     * Rename a table (changing the logical name of the table)
     *
     * @param catalogTable the table to be renamed
     * @param newTableName the new name for the table
     * @param statement the used statement
     */
    public abstract void renameTable( CatalogTable catalogTable, String newTableName, Statement statement ) throws TableAlreadyExistsException;

    /**
     * Rename a column of a table (changing the logical name of the column)
     *
     * @param catalogColumn the column to be renamed
     * @param newColumnName the new name for the column
     * @param statement the used statement
     */
    public abstract void renameColumn( CatalogColumn catalogColumn, String newColumnName, Statement statement ) throws ColumnAlreadyExistsException;

    /**
     * Create a new table
     *
     * @param schemaId the id of the schema to which the table belongs
     * @param tableName the name of the new table
     * @param columns all columns of the table
     * @param constraints all constraints for the table
     * @param ifNotExists whether to silently ignore if the table already exists
     * @param stores list of data stores on which to create a full placement for this table
     * @param placementType which placement type should be used for the initial placements
     * @param statement the used statement
     */
    public abstract void createTable( long schemaId, String tableName, List<ColumnInformation> columns, List<ConstraintInformation> constraints, boolean ifNotExists, List<DataStore> stores, PlacementType placementType, Statement statement ) throws TableAlreadyExistsException;

    /**
     * Adds a new constraint to a table
     *
     * @param constraintName the name of the constraint
     * @param constraintType the type of the constraint
     * @param columnNames the names of the columns for which to create the constraint
     * @param tableId the id of the table
     */
    public abstract void addConstraint( String constraintName, ConstraintType constraintType, List<String> columnNames, long tableId ) throws UnknownColumnException, GenericCatalogException;

    /**
     * Drop a schema
     *
     * @param databaseId the id of the database the schema belongs
     * @param schemaName the name of the schema to drop
     * @param ifExists whether to silently ignore if the schema does not exist
     * @param statement the used statement
     */
    public abstract void dropSchema( long databaseId, String schemaName, boolean ifExists, Statement statement ) throws SchemaNotExistException, DdlOnSourceException;

    /**
     * Drop a table
     *
     * @param catalogTable the table to be dropped
     * @param statement the used statement
     */
    public abstract void dropTable( CatalogTable catalogTable, Statement statement ) throws DdlOnSourceException;

    /**
     * Truncate a table
     *
     * @param catalogTable the table to be truncated
     * @param statement the used statement
     */
    public abstract void truncate( CatalogTable catalogTable, Statement statement );

    /**
     * Create a new type
     */
    public abstract void createType();

    /**
     * Drop a type
     */
    public abstract void dropType();

    /**
     * Create a new view
     */
    public abstract void createView();

    /**
     * Drop a view
     */
    public abstract void dropView();

    /**
     * Drop a function
     */
    public abstract void dropFunction();

    /**
     * Set a option
     */
    public abstract void setOption();


    /**
     * Helper class which holds all information required for creating a column,
     * decoupled from a specific query language
     */
    public static class ColumnInformation {

        public final String name;
        public final ColumnTypeInformation typeInformation;
        public final Collation collation;
        public final String defaultValue;
        public final int position;


        public ColumnInformation( String name, ColumnTypeInformation typeInformation, Collation collation, String defaultValue, int position ) {
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
    public static class ConstraintInformation {

        public final String name;
        public final ConstraintType type;
        public final List<String> columnNames;


        public ConstraintInformation( String name, ConstraintType type, List<String> columnNames ) {
            this.name = name;
            this.type = type;
            this.columnNames = columnNames;
        }

    }


    /**
     * Helper class, which holds all type information for a column
     * decoupled from the used query language
     */
    public static class ColumnTypeInformation {

        public final PolyType type;
        public final PolyType collectionType;
        public final Integer precision;
        public final Integer scale;
        public final Integer dimension;
        public final Integer cardinality;
        public final Boolean nullable;


        public ColumnTypeInformation( PolyType type, PolyType collectionType, Integer precision, Integer scale, Integer dimension, Integer cardinality, Boolean nullable ) {
            this.type = type;
            this.collectionType = collectionType;
            this.precision = precision == -1 ? null : precision;
            this.scale = scale == -1 ? null : scale;
            this.dimension = dimension == -1 ? null : dimension;
            this.cardinality = cardinality == -1 ? null : cardinality;
            this.nullable = nullable;
        }


        public static ColumnTypeInformation fromSqlDataTypeSpec( SqlDataTypeSpec sqlDataType ) {
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

}
