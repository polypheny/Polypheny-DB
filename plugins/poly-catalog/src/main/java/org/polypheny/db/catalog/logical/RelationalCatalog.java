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

package org.polypheny.db.catalog.logical;

import com.google.common.collect.ImmutableList;
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
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolyType;

// todo dl add object not null, synchronize
@Value
@SuperBuilder(toBuilder = true)
public class RelationalCatalog implements Serializable, LogicalRelationalCatalog {

    @Getter
    public BinarySerializer<RelationalCatalog> serializer = Serializable.builder.get().build( RelationalCatalog.class );

    @Serialize
    @Getter
    public Map<Long, LogicalTable> tables;

    @Serialize
    @Getter
    public Map<Long, LogicalColumn> columns;

    @Serialize
    @Getter
    public LogicalNamespace logicalNamespace;

    @Serialize
    @Getter
    public Map<Long, CatalogIndex> indexes;

    @Serialize
    @Getter
    public Map<Long, CatalogKey> keys;


    @Serialize
    @Getter
    public Map<long[], Long> keyColumns;

    @Serialize
    @Getter
    public Map<Long, CatalogConstraint> constraints;


    public IdBuilder idBuilder = IdBuilder.getInstance();

    @NonFinal
    @Builder.Default
    boolean openChanges = false;

    PropertyChangeSupport listeners = new PropertyChangeSupport( this );
    List<Long> tablesFlaggedForDeletion = new ArrayList<>();


    public RelationalCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("tables") Map<Long, LogicalTable> tables,
            @Deserialize("columns") Map<Long, LogicalColumn> columns,
            @Deserialize("indexes") Map<Long, CatalogIndex> indexes,
            @Deserialize("keys") Map<Long, CatalogKey> keys,
            @Deserialize("keyColumns") Map<long[], Long> keyColumns,
            @Deserialize("constraints") Map<Long, CatalogConstraint> constraints ) {
        this.logicalNamespace = logicalNamespace;

        this.tables = tables;
        this.columns = columns;
        this.indexes = indexes;
        this.keys = keys;
        this.keyColumns = keyColumns;
        this.constraints = constraints;
    }


    public RelationalCatalog( LogicalNamespace namespace ) {
        this( namespace, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }


    public void change() {
        openChanges = true;
    }


    @Override
    public RelationalCatalog copy() {
        return deserialize( serialize(), RelationalCatalog.class );
    }


    @Override
    public LogicalCatalog withLogicalNamespace( LogicalNamespace namespace ) {
        return toBuilder().logicalNamespace( namespace ).build();
    }


    @Override
    public LogicalTable addTable( String name, EntityType entityType, boolean modifiable ) {
        long id = idBuilder.getNewEntityId();
        LogicalTable table = new LogicalTable( id, name, logicalNamespace.id, entityType, null, modifiable, null );
        tables.put( id, table );
        return table;
    }


    @Override
    public long addView( String name, long namespaceId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, String query, QueryLanguage language ) {
        throw new NotImplementedException();
    }


    @Override
    public long addMaterializedView( String name, long namespaceId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) {
        throw new NotImplementedException();
    }


    @Override
    public void renameTable( long tableId, String name ) {
        tables.put( tableId, tables.get( tableId ).toBuilder().name( name ).build() );
    }


    @Override
    public void deleteTable( long tableId ) {
        for ( Long columnId : tables.get( tableId ).getColumnIds() ) {
            columns.remove( columnId );
        }
        tables.remove( tableId );
    }


    @Override
    public void setPrimaryKey( long tableId, Long keyId ) {
        tables.put( tableId, tables.get( tableId ).toBuilder().primaryKey( keyId ).build() );

        keys.put( keyId, new CatalogPrimaryKey( keys.get( keyId ) ) );
    }


    @Override
    public long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, long adapterId, IndexType type, String indexName ) {
        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
        if ( unique ) {
            // TODO: Check if the current values are unique
        }
        long id = idBuilder.getNewIndexId();
        synchronized ( this ) {
            indexes.put( id, new CatalogIndex(
                    id,
                    indexName,
                    unique,
                    method,
                    methodDisplayName,
                    type,
                    adapterId,
                    keyId,
                    Objects.requireNonNull( keys.get( keyId ) ),
                    null ) );
        }
        listeners.firePropertyChange( "index", null, keyId );
        return id;
    }


    private long getOrAddKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) {
        Long keyId = keyColumns.get( columnIds.stream().mapToLong( Long::longValue ).toArray() );
        if ( keyId != null ) {
            return keyId;
        }
        return addKey( tableId, columnIds, enforcementTime );
    }


    private long addKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) {
        LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
        long id = idBuilder.getNewKeyId();
        CatalogKey key = new CatalogKey( id, table.id, table.namespaceId, columnIds, enforcementTime );
        synchronized ( this ) {
            keys.put( id, key );
            keyColumns.put( columnIds.stream().mapToLong( Long::longValue ).toArray(), id );
        }
        listeners.firePropertyChange( "key", null, key );
        return id;
    }


    @Override
    public void setIndexPhysicalName( long indexId, String physicalName ) {
        indexes.put( indexId, indexes.get( indexId ).toBuilder().physicalName( physicalName ).build() );
    }


    @Override
    public void deleteIndex( long indexId ) {
        indexes.remove( indexId );
    }


    @Override
    public LogicalColumn addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) {
        long id = idBuilder.getNewFieldId();
        LogicalColumn column = new LogicalColumn( id, name, tableId, logicalNamespace.id, position, type, collectionsType, length, scale, dimension, cardinality, nullable, collation, null );
        columns.put( id, column );
        return column;
    }


    @Override
    public void renameColumn( long columnId, String name ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().name( name ).build() );
    }


    @Override
    public void setColumnPosition( long columnId, int position ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().position( position ).build() );
    }


    @Override
    public void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality ) {
        if ( scale != null && scale > length ) {
            throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
        }

        columns.put( columnId, columns.get( columnId ).toBuilder().type( type ).length( length ).scale( scale ).dimension( dimension ).cardinality( cardinality ).build() );
    }


    @Override
    public void setNullable( long columnId, boolean nullable ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().nullable( nullable ).build() );
    }


    @Override
    public void setCollation( long columnId, Collation collation ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().collation( collation ).build() );
    }


    @Override
    public void deleteColumn( long columnId ) {
        columns.remove( columnId );
    }


    @Override
    public LogicalColumn setDefaultValue( long columnId, PolyType type, String defaultValue ) {
        LogicalColumn column = columns.get( columnId ).toBuilder().defaultValue( new CatalogDefaultValue( columnId, type, defaultValue, "defaultValue" ) ).build();
        columns.put( columnId, column );
        return column;
    }


    @Override
    public void deleteDefaultValue( long columnId ) {
        columns.put( columnId, columns.get( columnId ).toBuilder().defaultValue( null ).build() );
    }


    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) {
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
    }


    private boolean isForeignKey( long key ) {
        return keys.values().stream().filter( k -> k instanceof CatalogForeignKey ).map( k -> (CatalogForeignKey) k ).anyMatch( k -> k.referencedKeyId == key );
    }


    private boolean isPrimaryKey( long key ) {
        return keys.values().stream().filter( k -> k instanceof CatalogPrimaryKey ).map( k -> (CatalogPrimaryKey) k ).anyMatch( k -> k.id == key );
    }


    /**
     * Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
     */
    private void deleteKeyIfNoLongerUsed( Long keyId ) {
        if ( keyId == null ) {
            return;
        }
        CatalogKey key = keys.get( keyId );
        LogicalTable table = tables.get( key.tableId );
        if ( table.primaryKey != null && table.primaryKey.equals( keyId ) ) {
            return;
        }
        if ( constraints.values().stream().anyMatch( c -> c.keyId == keyId ) ) {
            return;
        }
        if ( keys.values().stream().filter( k -> k instanceof CatalogForeignKey ).anyMatch( f -> f.id == keyId ) ) {
            return;
        }
        if ( indexes.values().stream().anyMatch( i -> i.keyId == keyId ) ) {
            return;
        }
        synchronized ( this ) {
            keys.remove( keyId );
            keyColumns.remove( key.columnIds.stream().mapToLong( Long::longValue ).toArray() );
        }
        listeners.firePropertyChange( "key", key, null );
    }


    private int getKeyUniqueCount( long keyId ) {
        CatalogKey key = keys.get( keyId );
        int count = 0;
        if ( isPrimaryKey( keyId ) ) {
            count++;
        }

        for ( CatalogConstraint constraint : constraints.values().stream().filter( c -> c.keyId == keyId ).collect( Collectors.toList() ) ) {
            if ( constraint.type == ConstraintType.UNIQUE ) {
                count++;
            }
        }

        for ( CatalogIndex index : indexes.values().stream().filter( i -> i.keyId == keyId ).collect( Collectors.toList() ) ) {
            if ( index.unique ) {
                count++;
            }
        }

        return count;
    }


    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) {
        LogicalTable table = tables.get( tableId );
        List<CatalogKey> childKeys = keys.values().stream().filter( k -> k.tableId == referencesTableId ).collect( Collectors.toList() );

        for ( CatalogKey refKey : childKeys ) {
            if ( refKey.columnIds.size() == referencesIds.size() && refKey.columnIds.containsAll( referencesIds ) && new HashSet<>( referencesIds ).containsAll( refKey.columnIds ) ) {

                int i = 0;
                for ( long referencedColumnId : refKey.columnIds ) {
                    LogicalColumn referencingColumn = columns.get( columnIds.get( i++ ) );
                    LogicalColumn referencedColumn = columns.get( referencedColumnId );
                    if ( referencedColumn.type != referencingColumn.type ) {
                        throw new GenericRuntimeException( "The data type of the referenced columns does not match the data type of the referencing column: %s != %s", referencingColumn.type.name(), referencedColumn.type );
                    }
                }
                // TODO same keys for key and foreign key
                if ( getKeyUniqueCount( refKey.id ) > 0 ) {
                    long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_COMMIT );
                    CatalogForeignKey key = new CatalogForeignKey(
                            keyId,
                            constraintName,
                            tableId,
                            table.namespaceId,
                            refKey.id,
                            refKey.tableId,
                            refKey.namespaceId,
                            columnIds,
                            referencesIds,
                            onUpdate,
                            onDelete );
                    synchronized ( this ) {
                        keys.put( keyId, key );
                    }
                    return;
                }
            }
        }

    }


    @Override
    public void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) {
        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
        // Check if there is already a unique constraint
        List<CatalogConstraint> catalogConstraints = constraints.values().stream()
                .filter( c -> c.keyId == keyId && c.type == ConstraintType.UNIQUE )
                .collect( Collectors.toList() );
        if ( catalogConstraints.size() > 0 ) {
            throw new GenericRuntimeException( "There is already a unique constraint!" );
        }
        long id = idBuilder.getNewConstraintId();
        synchronized ( this ) {
            constraints.put( id, new CatalogConstraint( id, keyId, ConstraintType.UNIQUE, constraintName, Objects.requireNonNull( keys.get( keyId ) ) ) );
        }
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
    }


    @Override
    public void deleteForeignKey( long foreignKeyId ) {
        CatalogForeignKey catalogForeignKey = (CatalogForeignKey) keys.get( foreignKeyId );
        synchronized ( this ) {
            keys.remove( catalogForeignKey.id );
            deleteKeyIfNoLongerUsed( catalogForeignKey.id );
        }
    }


    @Override
    public void deleteConstraint( long constraintId ) {
        CatalogConstraint catalogConstraint = Objects.requireNonNull( constraints.get( constraintId ) );
        //CatalogCombinedKey key = getCombinedKey( catalogConstraint.keyId );
        if ( catalogConstraint.type == ConstraintType.UNIQUE && isForeignKey( catalogConstraint.keyId ) ) {
            if ( getKeyUniqueCount( catalogConstraint.keyId ) < 2 ) {
                throw new GenericRuntimeException( "This key is referenced by at least one foreign key which requires this key to be unique. Unable to drop unique constraint." );
            }
        }
        synchronized ( this ) {
            constraints.remove( catalogConstraint.id );
        }
        deleteKeyIfNoLongerUsed( catalogConstraint.keyId );

    }


    @Override
    public void deleteViewDependencies( CatalogView catalogView ) {
        for ( long id : catalogView.getUnderlyingTables().keySet() ) {
            LogicalTable old = tables.get( id );
            List<Long> connectedViews = old.connectedViews.stream().filter( e -> e != catalogView.id ).collect( Collectors.toList() );

            LogicalTable table = old.toBuilder().connectedViews( ImmutableList.copyOf( connectedViews ) ).build();

            synchronized ( this ) {
                tables.put( id, table );
            }
            listeners.firePropertyChange( "table", old, table );
        }
    }


    @Override
    public void updateMaterializedViewRefreshTime( long materializedViewId ) {

        CatalogMaterializedView old = (CatalogMaterializedView) tables.get( materializedViewId );

        MaterializedCriteria materializedCriteria = old.getMaterializedCriteria();
        materializedCriteria.setLastUpdate( new Timestamp( System.currentTimeMillis() ) );

        synchronized ( this ) {
            tables.put( materializedViewId, old.toBuilder().materializedCriteria( materializedCriteria ).build() );
        }

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
