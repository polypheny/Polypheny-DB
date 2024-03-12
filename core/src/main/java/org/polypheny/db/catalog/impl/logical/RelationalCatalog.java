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

package org.polypheny.db.catalog.impl.logical;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeSupport;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalGenericKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalKey.EnforcementTime;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.util.CatalogEvent;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@Value
@SuperBuilder(toBuilder = true)
public class RelationalCatalog implements PolySerializable, LogicalRelationalCatalog {

    @Getter
    public BinarySerializer<RelationalCatalog> serializer = PolySerializable.buildSerializer( RelationalCatalog.class );

    public IdBuilder idBuilder = IdBuilder.getInstance();

    @Serialize
    @Getter
    public Map<Long, LogicalTable> tables;

    @Serialize
    @Getter
    public Map<Long, LogicalColumn> columns;

    @Getter
    public Map<Long, AlgNode> nodes;

    @Serialize
    @Getter
    public LogicalNamespace logicalNamespace;

    @Serialize
    @Getter
    public Map<Long, LogicalIndex> indexes;

    // while keys "belong" to a specific table, they can reference other namespaces, atm they are place here, might change later
    @Serialize
    @Getter
    public Map<Long, LogicalKey> keys;


    @Serialize
    @Getter
    public Map<Long, LogicalConstraint> constraints;


    List<Long> tablesFlaggedForDeletion = new ArrayList<>();


    public RelationalCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("tables") Map<Long, LogicalTable> tables,
            @Deserialize("columns") Map<Long, LogicalColumn> columns,
            @Deserialize("indexes") Map<Long, LogicalIndex> indexes,
            @Deserialize("keys") Map<Long, LogicalKey> keys,
            @Deserialize("constraints") Map<Long, LogicalConstraint> constraints ) {
        this.logicalNamespace = logicalNamespace;

        this.tables = new ConcurrentHashMap<>( tables );
        this.columns = new ConcurrentHashMap<>( columns );
        this.indexes = new ConcurrentHashMap<>( indexes );
        this.keys = new ConcurrentHashMap<>( keys );
        this.constraints = new ConcurrentHashMap<>( constraints );
        this.nodes = new ConcurrentHashMap<>();
        listeners.addPropertyChangeListener( Catalog.getInstance().getChangeListener() );
    }


    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public void change( CatalogEvent event, Object oldValue, Object newValue ) {
        listeners.firePropertyChange( event.name(), oldValue, newValue );
    }


    public RelationalCatalog( LogicalNamespace namespace ) {
        this( namespace, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), RelationalCatalog.class );
    }


    @Override
    public LogicalCatalog withLogicalNamespace( LogicalNamespace namespace ) {
        return toBuilder().logicalNamespace( namespace ).build();
    }


    @Override
    public LogicalTable addTable( String name, EntityType entityType, boolean modifiable ) {
        long id = idBuilder.getNewLogicalId();
        LogicalTable table = new LogicalTable( id, name, logicalNamespace.id, entityType, null, modifiable );
        tables.put( id, table );
        change( CatalogEvent.LOGICAL_REL_ENTITY_CREATED, null, id );
        return table;
    }


    @Override
    public LogicalView addView( String name, long namespaceId, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, List<Long> connectedViews, AlgDataType fieldList, String query, QueryLanguage language ) {
        long id = idBuilder.getNewLogicalId();

        LogicalView view = new LogicalView( id, name, namespaceId, EntityType.VIEW, query, algCollation, underlyingTables, language );

        tables.put( id, view );
        nodes.put( id, definition );
        change( CatalogEvent.VIEW_CREATED, null, id );
        return view;
    }


    @Override
    public LogicalMaterializedView addMaterializedView( final String name, long namespaceId, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) {
        long id = idBuilder.getNewLogicalId();

        LogicalMaterializedView materializedViewTable = new LogicalMaterializedView(
                id,
                name,
                namespaceId,
                query,
                algCollation,
                underlyingTables,
                language,
                materializedCriteria,
                ordered
        );

        tables.put( id, materializedViewTable );
        nodes.put( id, definition );
        change( CatalogEvent.MATERIALIZED_VIEW_CREATED, null, id );
        return materializedViewTable;
    }


    @Override
    public void renameTable( long tableId, String name ) {
        tables.put( tableId, tables.get( tableId ).toBuilder().name( name ).build() );
        change( CatalogEvent.LOGICAL_REL_ENTITY_RENAMED, tableId, name );
    }


    @Override
    public void deleteTable( long tableId ) {
        for ( Long columnId : tables.get( tableId ).getColumnIds() ) {
            columns.remove( columnId );
        }
        tables.remove( tableId );
        change( CatalogEvent.LOGICAL_REL_ENTITY_DROPPED, tableId, null );
    }


    @Override
    public void setPrimaryKey( long tableId, Long keyId ) {
        tables.put( tableId, tables.get( tableId ).toBuilder().primaryKey( keyId ).build() );

        keys.put( keyId, new LogicalPrimaryKey( keys.get( keyId ) ) );
        change( CatalogEvent.PRIMARY_KEY_CREATED, tableId, keyId );
    }


    @Override
    public LogicalIndex addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, long adapterId, IndexType type, String indexName ) {
        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
        if ( unique ) {
            // TODO: Check if the current values are unique
        }
        long id = idBuilder.getNewIndexId();
        LogicalIndex index = new LogicalIndex(
                id,
                indexName,
                unique,
                method,
                methodDisplayName,
                type,
                adapterId,
                keyId,
                Objects.requireNonNull( keys.get( keyId ) ),
                null );
        synchronized ( this ) {
            indexes.put( id, index );
        }
        listeners.firePropertyChange( "index", null, keyId );
        change( CatalogEvent.INDEX_CREATED, null, id );
        return index;
    }


    private long getOrAddKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) {
        return Catalog.snapshot()
                .rel()
                .getKeys( columnIds.stream().mapToLong( Long::longValue ).toArray() )
                .map( k -> k.id )
                .orElse( addKey( tableId, columnIds, enforcementTime ) );
    }


    private long addKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) {
        LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
        long id = idBuilder.getNewKeyId();
        LogicalKey key = new LogicalGenericKey( id, table.id, table.namespaceId, columnIds, enforcementTime );
        synchronized ( this ) {
            keys.put( id, key );
        }
        change( CatalogEvent.KEY_CREATED, null, id );
        return id;
    }


    @Override
    public void setIndexPhysicalName( long indexId, String physicalName ) {
        indexes.put( indexId, indexes.get( indexId ).toBuilder().physicalName( physicalName ).build() );
        change( CatalogEvent.INDEX_CREATED, indexId, physicalName );
    }


    @Override
    public void deleteIndex( long indexId ) {
        indexes.remove( indexId );
        change( CatalogEvent.INDEX_DROPPED, indexId, null );
    }


    @Override
    public void deleteKey( long id ) {
        keys.remove( id );
        change( CatalogEvent.KEY_DROPPED, id, null );
    }


    @Override
    public LogicalColumn addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) {
        long id = idBuilder.getNewFieldId();
        LogicalColumn column = new LogicalColumn( id, name, tableId, logicalNamespace.id, position, type, collectionsType, length, scale, dimension, cardinality, nullable, collation, null );
        columns.put( id, column );
        change( CatalogEvent.LOGICAL_REL_FIELD_CREATED, null, id );
        return column;
    }


    @Override
    public void renameColumn( long columnId, String name ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().name( name ).build() );
        change( CatalogEvent.LOGICAL_REL_ENTITY_RENAMED, columnId, name );
    }


    @Override
    public void setColumnPosition( long columnId, int position ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().position( position ).build() );
        change( CatalogEvent.LOGICAL_REL_FIELD_POSITION_CHANGED, columnId, position );
    }


    @Override
    public void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality ) {
        if ( scale != null && scale > length ) {
            throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
        }

        columns.put( columnId, columns.get( columnId ).toBuilder().type( type ).length( length ).scale( scale ).dimension( dimension ).cardinality( cardinality ).build() );
        change( CatalogEvent.LOGICAL_REL_FIELD_TYPE_CHANGED, columnId, type );
    }


    @Override
    public void setNullable( long columnId, boolean nullable ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().nullable( nullable ).build() );
        change( CatalogEvent.LOGICAL_REL_FIELD_NULLABILITY_CHANGED, columnId, nullable );
    }


    @Override
    public void setCollation( long columnId, Collation collation ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().collation( collation ).build() );
        change( CatalogEvent.LOGICAL_REL_FIELD_COLLATION_CHANGED, columnId, collation );
    }


    @Override
    public void deleteColumn( long columnId ) {
        columns.remove( columnId );
        change( CatalogEvent.LOGICAL_REL_FIELD_DROPPED, columnId, null );
    }


    @Override
    public LogicalColumn setDefaultValue( long columnId, PolyType type, PolyValue defaultValue ) {
        LogicalColumn column = columns.get( columnId ).toBuilder().defaultValue( new LogicalDefaultValue( columnId, type, defaultValue, "defaultValue" ) ).build();
        columns.put( columnId, column );
        change( CatalogEvent.LOGICAL_REL_FIELD_DEFAULT_VALUE_CHANGED, columnId, defaultValue );
        return column;
    }


    @Override
    public void deleteDefaultValue( long columnId ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().defaultValue( null ).build() );
        change( CatalogEvent.LOGICAL_REL_FIELD_DEFAULT_VALUE_DROPPED, columnId, null );
    }


    @Override
    public LogicalTable addPrimaryKey( long tableId, List<Long> columnIds ) {
        if ( columnIds.stream().anyMatch( id -> columns.get( id ).nullable ) ) {
            throw new GenericRuntimeException( "Primary key is not allowed to use nullable columns." );
        }

        // TODO: Check if the current values are unique

        // Check if there is already a primary key defined for this table and if so, delete it.
        LogicalTable table = tables.get( tableId );

        if ( table.primaryKey != null ) {
            // CatalogCombinedKey combinedKey = getCombinedKey( table.primaryKey );
            if ( getKeyUniqueCount( table.primaryKey ) == 1 && isForeignKey( table.primaryKey ) ) {
                // This primary key is the only constraint for the uniqueness of this key.
                throw new GenericRuntimeException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key, first drop the foreign keys or create a unique constraint." );
            }
            synchronized ( this ) {
                setPrimaryKey( tableId, null );
                deleteKeyIfNoLongerUsed( table.primaryKey );
            }
        }
        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
        setPrimaryKey( tableId, keyId );
        change( CatalogEvent.PRIMARY_KEY_CREATED, tableId, keyId );
        return tables.get( tableId );
    }


    private boolean isForeignKey( long key ) {
        return keys.values().stream().filter( k -> k instanceof LogicalForeignKey ).map( k -> (LogicalForeignKey) k ).anyMatch( k -> k.referencedKeyId == key );
    }


    private boolean isPrimaryKey( long key ) {
        return keys.values().stream().filter( k -> k instanceof LogicalPrimaryKey ).map( k -> (LogicalPrimaryKey) k ).anyMatch( k -> k.id == key );
    }


    /**
     * Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
     */
    private void deleteKeyIfNoLongerUsed( Long keyId ) {
        if ( keyId == null ) {
            return;
        }
        LogicalKey key = keys.get( keyId );
        LogicalTable table = tables.get( key.entityId );
        if ( table.primaryKey != null && table.primaryKey.equals( keyId ) ) {
            return;
        }
        if ( constraints.values().stream().anyMatch( c -> c.keyId == keyId ) ) {
            return;
        }
        if ( keys.values().stream().filter( k -> k instanceof LogicalForeignKey ).anyMatch( f -> f.id == keyId ) ) {
            return;
        }
        if ( indexes.values().stream().anyMatch( i -> i.keyId == keyId ) ) {
            return;
        }
        synchronized ( this ) {
            keys.remove( keyId );
        }
        change( CatalogEvent.KEY_DROPPED, keyId, null );
    }


    private int getKeyUniqueCount( long keyId ) {
        int count = 0;
        if ( Catalog.snapshot().rel().getPrimaryKey( keyId ).isPresent() ) {
            count++;
        }

        for ( LogicalConstraint constraint : constraints.values().stream().filter( c -> c.keyId == keyId ).toList() ) {
            if ( constraint.type == ConstraintType.UNIQUE ) {
                count++;
            }
        }

        for ( LogicalIndex index : indexes.values().stream().filter( i -> i.keyId == keyId ).toList() ) {
            if ( index.unique ) {
                count++;
            }
        }

        return count;
    }


    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) {
        if ( tableId == referencesTableId ) {
            throw new GenericRuntimeException( "A foreign key can not reference the same table." );
        }

        LogicalTable table = tables.get( tableId );
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        List<LogicalKey> childKeys = snapshot.rel().getTableKeys( referencesTableId );

        for ( LogicalKey refKey : childKeys ) {
            if ( refKey.fieldIds.size() != referencesIds.size() || !refKey.fieldIds.containsAll( referencesIds ) || !new HashSet<>( referencesIds ).containsAll( refKey.fieldIds ) ) {
                continue;
            }
            int i = 0;
            for ( long referencedColumnId : refKey.fieldIds ) {
                LogicalColumn referencingColumn = snapshot.rel().getColumn( columnIds.get( i++ ) ).orElseThrow();
                LogicalColumn referencedColumn = snapshot.rel().getColumn( referencedColumnId ).orElseThrow();
                if ( referencedColumn.type != referencingColumn.type ) {
                    throw new GenericRuntimeException( "The data type of the referenced columns does not match the data type of the referencing column: %s != %s", referencingColumn.type.name(), referencedColumn.type );
                }
            }
            long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_COMMIT );

            LogicalForeignKey key = new LogicalForeignKey(
                    keyId,
                    constraintName,
                    tableId,
                    table.namespaceId,
                    refKey.id,
                    refKey.entityId,
                    refKey.namespaceId,
                    columnIds,
                    referencesIds,
                    onUpdate,
                    onDelete );
            synchronized ( this ) {
                keys.put( keyId, key );
                change( CatalogEvent.FOREIGN_KEY_CREATED, null, keyId );
            }
            return;
        }
        throw new GenericRuntimeException( "Referenced columns are not defined as UNIQUE, which is required for foreign keys." );

    }


    @Override
    public LogicalTable addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) {
        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
        // Check if there is already a unique constraint
        List<LogicalConstraint> logicalConstraints = constraints.values().stream()
                .filter( c -> c.keyId == keyId && c.type == ConstraintType.UNIQUE )
                .toList();
        if ( !logicalConstraints.isEmpty() ) {
            throw new GenericRuntimeException( "There is already a unique constraint!" );
        }
        long id = idBuilder.getNewConstraintId();
        synchronized ( this ) {
            constraints.put( id, new LogicalConstraint( id, keyId, ConstraintType.UNIQUE, constraintName, Objects.requireNonNull( keys.get( keyId ) ) ) );
            change( CatalogEvent.CONSTRAINT_CREATED, null, id );
        }
        return tables.get( tableId );
    }


    @Override
    public void deletePrimaryKey( long tableId ) {
        LogicalTable table = tables.get( tableId );

        // TODO: Check if the currently stored values are unique
        if ( table.primaryKey != null ) {
            // Check if this primary key is required to maintain to uniqueness
            // CatalogCombinedKey key = getCombinedKey( table.primaryKey );
            if ( isForeignKey( table.primaryKey ) ) {
                if ( getKeyUniqueCount( table.primaryKey ) < 2 ) {
                    throw new GenericRuntimeException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key either drop the foreign key or create a unique constraint." );
                }
            }

            setPrimaryKey( tableId, null );
            deleteKeyIfNoLongerUsed( table.primaryKey );
        }
        change( CatalogEvent.PRIMARY_KEY_DROPPED, tableId, null );
    }


    @Override
    public void deleteForeignKey( long foreignKeyId ) {
        LogicalForeignKey logicalForeignKey = (LogicalForeignKey) keys.get( foreignKeyId );
        synchronized ( this ) {
            deleteKeyIfNoLongerUsed( logicalForeignKey.id );
        }
        change( CatalogEvent.FOREIGN_KEY_DROPPED, foreignKeyId, null );
    }


    @Override
    public void deleteConstraint( long constraintId ) {
        LogicalConstraint logicalConstraint = constraints.get( constraintId );
        synchronized ( this ) {
            constraints.remove( logicalConstraint.id );
        }
        deleteKeyIfNoLongerUsed( logicalConstraint.keyId );
        change( CatalogEvent.CONSTRAINT_DROPPED, constraintId, null );
    }


    @Override
    public void updateMaterializedViewRefreshTime( long materializedViewId ) {

        LogicalMaterializedView old = (LogicalMaterializedView) tables.get( materializedViewId );

        MaterializedCriteria materializedCriteria = old.getMaterializedCriteria();
        materializedCriteria.setLastUpdate( new Timestamp( System.currentTimeMillis() ) );

        synchronized ( this ) {
            tables.put( materializedViewId, old.toBuilder().materializedCriteria( materializedCriteria ).build() );
        }
        change( CatalogEvent.MATERIALIZED_VIEW_UPDATED, materializedViewId, null );
    }


    @Override
    public void flagTableForDeletion( long tableId, boolean flag ) {
        if ( flag && !tablesFlaggedForDeletion.contains( tableId ) ) {
            tablesFlaggedForDeletion.add( tableId );
        } else if ( !flag && tablesFlaggedForDeletion.contains( tableId ) ) {
            tablesFlaggedForDeletion.remove( tableId );
        }
    }


    @Override
    public boolean isTableFlaggedForDeletion( long tableId ) {
        return tablesFlaggedForDeletion.contains( tableId );
    }

}
