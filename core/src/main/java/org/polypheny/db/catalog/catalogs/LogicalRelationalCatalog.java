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

package org.polypheny.db.catalog.catalogs;

import io.activej.serializer.annotations.SerializeClass;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.impl.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@SerializeClass(subclasses = { RelationalCatalog.class })
public interface LogicalRelationalCatalog extends LogicalCatalog {

    /**
     * Adds a table to a specified schema.
     *
     * @param name The name of the table to add
     * @param entityType The table type
     * @param modifiable Whether the content of the table can be modified
     * @return The id of the inserted table
     */
    LogicalTable addTable( String name, EntityType entityType, boolean modifiable );


    /**
     * Adds a view to a specified schema.
     *
     * @param name The name of the view to add
     * @param namespaceId The id of the schema
     * @param modifiable Whether the content of the table can be modified
     * @param definition {@link AlgNode} used to create Views
     * @param underlyingTables all tables and columns used within the view
     * @param fieldList all columns used within the View
     * @return The id of the inserted table
     */
    LogicalView addView( String name, long namespaceId, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, List<Long> connectedViews, AlgDataType fieldList, String query, QueryLanguage language );

    /**
     * Adds a materialized view to a specified schema.
     *
     * @param name of the view to add
     * @param namespaceId id of the schema
     * @param definition {@link AlgNode} used to create Views
     * @param algCollation relCollation used for materialized view
     * @param underlyingTables all tables and columns used within the view
     * @param fieldList all columns used within the View
     * @param materializedCriteria Information like freshness and last updated
     * @param query used to define materialized view
     * @param language query language used to define materialized view
     * @param ordered if materialized view is ordered or not
     * @return id of the inserted materialized view
     */
    LogicalMaterializedView addMaterializedView( String name, long namespaceId, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered );

    /**
     * Renames a table
     *
     * @param tableId The if of the table to rename
     * @param name New name of the table
     */
    void renameTable( long tableId, String name );

    /**
     * Delete the specified table. Columns need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    void deleteTable( long tableId );

    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    void setPrimaryKey( long tableId, Long keyId );

    /**
     * Adds a column.
     *
     * @param name The name of the column
     * @param tableId The id of the corresponding table
     * @param position The ordinal position of the column (starting with 1)
     * @param type The type of the column
     * @param length The length of the field (if applicable, else null)
     * @param scale The number of digits after the decimal point (if applicable, else null)
     * @param nullable Weather the column can contain null values
     * @param collation The collation of the field (if applicable, else null)
     * @return The id of the inserted column
     */
    LogicalColumn addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation );


    /**
     * Renames a column
     *
     * @param columnId The if of the column to rename
     * @param name New name of the column
     */
    void renameColumn( long columnId, String name );

    /**
     * Change the position of the column.
     *
     * @param columnId The id of the column for which to change the position
     * @param position The new position of the column
     */
    void setColumnPosition( long columnId, int position );

    /**
     * Change the data type of a column.
     *
     * @param columnId The id of the column
     * @param type The new type of the column
     */
    void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer precision, Integer dimension, Integer cardinality );

    /**
     * Change nullability of the column (weather the column allows null values).
     *
     * @param columnId The id of the column
     * @param nullable True if the column should allow null values, false if not.
     */
    void setNullable( long columnId, boolean nullable );

    /**
     * Set the collation of a column.
     * If the column already has the specified collation set, this method is a NoOp.
     *
     * @param columnId The id of the column
     * @param collation The collation to set
     */
    void setCollation( long columnId, Collation collation );


    /**
     * Delete the specified column. This also deletes a default value in case there is one defined for this column.
     *
     * @param columnId The id of the column to delete
     */
    void deleteColumn( long columnId );

    /**
     * Adds a default value for a column. If there already is a default values, it being replaced.
     *
     * @param columnId The id of the column
     * @param type The type of the default value
     * @param defaultValue True if the column should allow null values, false if not.
     * @return
     */
    LogicalColumn setDefaultValue( long columnId, PolyType type, PolyValue defaultValue );

    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    void deleteDefaultValue( long columnId );


    /**
     * Adds a primary key
     *
     * @param tableId The id of the table
     * @param columnIds The id of key which will be part of the primary keys
     * @return
     */
    LogicalTable addPrimaryKey( long tableId, List<Long> columnIds );


    /**
     * Adds a unique foreign key constraint.
     *
     * @param tableId The id of the table
     * @param columnIds The id of the columns which are part of the foreign key
     * @param referencesTableId The if of the referenced table
     * @param referencesIds The id of columns forming the key referenced by this key
     * @param constraintName The name of the constraint
     * @param onUpdate The option for updates
     * @param onDelete The option for deletes
     */
    void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete );

    /**
     * Adds a unique constraint.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @param columnIds A list of column ids
     * @return
     */
    LogicalTable addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds );

    /**
     * Deletes the specified primary key (including the entry in the key table). If there is an index on this key, make sure to delete it first.
     *
     * @param tableId The id of the key to drop
     */
    void deletePrimaryKey( long tableId );

    /**
     * Delete the specified foreign key (does not delete the referenced key).
     *
     * @param foreignKeyId The id of the foreign key to delete
     */
    void deleteForeignKey( long foreignKeyId );

    /**
     * Delete the specified constraint.
     * For deleting foreign keys, use {@link #deleteForeignKey(long)}.
     *
     * @param constraintId The id of the constraint to delete
     */
    void deleteConstraint( long constraintId );


    /**
     * Updates the last time a materialized view has been refreshed.
     *
     * @param materializedViewId id of the materialized view
     */
    void updateMaterializedViewRefreshTime( long materializedViewId );


    /**
     * Flags the table for deletion.
     * This method should be executed on a partitioned table before we run a DROP TABLE statement.
     *
     * @param tableId table to be flagged for deletion
     * @param flag true if it should be flagged, false if flag should be removed
     */
    void flagTableForDeletion( long tableId, boolean flag );

    /**
     * Is used to detect if a table is flagged for deletion.
     * Effectively checks if a drop of this table is currently in progress.
     * This is needed to ensure that there aren't any constraints when recursively removing a table and all placements and partitions.
     *
     * @param tableId table to be checked
     * @return If table is flagged for deletion or not
     */
    boolean isTableFlaggedForDeletion( long tableId );

    /**
     * Adds an index over the specified columns
     *
     * @param tableId The id of the table
     * @param columnIds A list of column ids
     * @param unique Weather the index is unique
     * @param method Name of the index method (e.g. btree_unique)
     * @param methodDisplayName Display name of the index method (e.g. BTREE)
     * @param adapterId ID of the data store where the index is located (0 for Polypheny-DB itself)
     * @param type The type of index (manual, automatic)
     * @param indexName The name of the index
     * @return The id of the created index
     */
    LogicalIndex addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, long adapterId, IndexType type, String indexName );

    /**
     * Set physical index name.
     *
     * @param indexId The id of the index
     * @param physicalName The physical name to be set
     */
    void setIndexPhysicalName( long indexId, String physicalName );

    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    void deleteIndex( long indexId );

    Map<Long, LogicalTable> getTables();

    Map<Long, LogicalColumn> getColumns();

    Map<Long, LogicalIndex> getIndexes();

    Map<Long, LogicalKey> getKeys();

    Map<Long, LogicalConstraint> getConstraints();

    Map<Long, AlgNode> getNodes();

    void deleteKey( long id );

}
