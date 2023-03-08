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

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeSupport;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.PusherMap;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolyType;

@Value
@SuperBuilder(toBuilder = true)
public class RelationalCatalog implements Serializable, LogicalRelationalCatalog {

    @Getter
    public BinarySerializer<RelationalCatalog> serializer = Serializable.builder.get().build( RelationalCatalog.class );

    @Serialize
    public PusherMap<Long, LogicalTable> tables;

    @Serialize
    public PusherMap<Long, LogicalColumn> columns;

    @Serialize
    @Getter
    public LogicalNamespace logicalNamespace;

    @Serialize
    public Map<Long, CatalogIndex> indexes;

    @Serialize
    public Map<Long, CatalogKey> keys;

    @Serialize
    public Map<long[], Long> keyColumns;


    public IdBuilder idBuilder = IdBuilder.getInstance();
    ConcurrentHashMap<String, LogicalTable> names;

    @NonFinal
    @Builder.Default
    boolean openChanges = false;

    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public RelationalCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("tables") Map<Long, LogicalTable> tables,
            @Deserialize("columns") Map<Long, LogicalColumn> columns,
            @Deserialize("indexes") Map<Long, CatalogIndex> indexes,
            @Deserialize("keys") Map<Long, CatalogKey> keys,
            @Deserialize("keyColumns") Map<long[], Long> keyColumns ) {
        this.logicalNamespace = logicalNamespace;

        this.tables = new PusherMap<>( tables );
        this.columns = new PusherMap<>( columns );
        this.indexes = indexes;
        this.keys = keys;
        this.keyColumns = keyColumns;

        this.names = new ConcurrentHashMap<>();
        this.tables.addRowConnection( this.names, ( k, v ) -> logicalNamespace.caseSensitive ? v.name : v.name.toLowerCase(), ( k, v ) -> v );

    }


    public RelationalCatalog( LogicalNamespace namespace ) {
        this( namespace, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>() );
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
    public long addTable( String name, EntityType entityType, boolean modifiable ) {
        long id = idBuilder.getNewEntityId();
        LogicalTable table = new LogicalTable( id, name, List.of(), logicalNamespace.id, logicalNamespace.name, entityType, null, List.of(), modifiable, null, List.of() );
        tables.put( id, table );
        return id;
    }


    @Override
    public long addView( String name, long namespaceId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, String query, QueryLanguage language ) {
        return 0;
    }


    @Override
    public long addMaterializedView( String name, long namespaceId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void renameTable( long tableId, String name ) {

    }


    @Override
    public void deleteTable( long tableId ) {

    }


    @Override
    public void setTableOwner( long tableId, long ownerId ) {

    }


    @Override
    public void setPrimaryKey( long tableId, Long keyId ) {

    }


    @Override
    public long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, long adapterId, IndexType type, String indexName ) throws GenericCatalogException {
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
        try {
            return addKey( tableId, columnIds, enforcementTime );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }


    private long addKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) throws GenericCatalogException {
        try {
            LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
            long id = idBuilder.getNewKeyId();
            CatalogKey key = new CatalogKey( id, table.id, table.namespaceId, columnIds, enforcementTime );
            synchronized ( this ) {
                keys.put( id, key );
                keyColumns.put( columnIds.stream().mapToLong( Long::longValue ).toArray(), id );
            }
            listeners.firePropertyChange( "key", null, key );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public void setIndexPhysicalName( long indexId, String physicalName ) {

    }


    @Override
    public void deleteIndex( long indexId ) {

    }


    @Override
    public long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) {
        long id = idBuilder.getNewFieldId();
        LogicalColumn column = new LogicalColumn( id, name, tableId, logicalNamespace.id, position, type, collectionsType, length, scale, dimension, cardinality, nullable, collation, null );
        columns.put( id, column );
        tables.put( tableId, tables.get( tableId ).withAddedColumn( column ) );
        return id;
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
    public void deleteColumn( long columnId ) {

    }


    @Override
    public void setDefaultValue( long columnId, PolyType type, String defaultValue ) {

    }


    @Override
    public void deleteDefaultValue( long columnId ) {

    }


    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {

    }


    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException {

    }


    @Override
    public void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) throws GenericCatalogException {

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
    public void deleteViewDependencies( CatalogView catalogView ) {

    }


    @Override
    public void updateMaterializedViewRefreshTime( long materializedViewId ) {

    }


    @Override
    public void flagTableForDeletion( long tableId, boolean flag ) {

    }


    @Override
    public boolean isTableFlaggedForDeletion( long tableId ) {
        return false;
    }

}
