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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import lombok.With;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.PusherMap;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownIndexIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolyType;

@Value
@With
public class RelationalCatalog implements Serializable, LogicalRelationalCatalog {

    @Getter
    public BinarySerializer<RelationalCatalog> serializer = Serializable.builder.get().build( RelationalCatalog.class );

    @Serialize
    public PusherMap<Long, LogicalTable> tables;

    @Serialize
    public PusherMap<Long, LogicalColumn> columns;

    @Getter
    public LogicalNamespace logicalNamespace;

    @Serialize
    public Map<Long, CatalogIndex> indexes;

    @Serialize
    public Map<Long, CatalogKey> keys;

    public Map<long[], Long> keyColumns;

    @Serialize
    public IdBuilder idBuilder;
    ConcurrentHashMap<String, LogicalTable> names;

    @NonFinal
    boolean openChanges = false;

    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public RelationalCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("idBuilder") IdBuilder idBuilder,
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

        this.idBuilder = idBuilder;
    }


    public RelationalCatalog( LogicalNamespace namespace, IdBuilder idBuilder ) {
        this( namespace, idBuilder, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }


    public void change() {
        openChanges = true;
    }


    @Override
    public RelationalCatalog copy() {
        return deserialize( serialize(), RelationalCatalog.class );
    }


    @Override
    public boolean checkIfExistsEntity( String entityName ) {
        return false;
    }


    @Override
    public boolean checkIfExistsEntity( long tableId ) {
        return false;
    }


    @Override
    public LogicalEntity getEntity( String name ) {
        return names.get( name );
    }


    @Override
    public LogicalEntity getEntity( long id ) {
        return tables.get( id );
    }


    @Override
    public List<LogicalTable> getTables( @Nullable Pattern name ) {
        if ( name == null ) {
            return List.copyOf( tables.values() );
        }
        return tables
                .values()
                .stream()
                .filter( t -> logicalNamespace.caseSensitive ?
                        t.name.toLowerCase().matches( name.toRegex() ) :
                        t.name.matches( name.toRegex() ) ).collect( Collectors.toList() );
    }


    @Override
    public LogicalTable getTable( long tableId ) {
        return null;
    }


    @Override
    public LogicalTable getTable( String tableName ) throws UnknownTableException {
        return null;
    }


    @Override
    public LogicalTable getTableFromPartition( long partitionId ) {
        return null;
    }


    @Override
    public long addTable( String name, int ownerId, EntityType entityType, boolean modifiable ) {
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
    public List<CatalogIndex> getIndexes( CatalogKey key ) {
        return indexes.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogIndex> getForeignKeys( CatalogKey key ) {
        return indexes.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) {
        if ( !onlyUnique ) {
            return indexes.values().stream().filter( i -> i.key.tableId == tableId ).collect( Collectors.toList() );
        } else {
            return indexes.values().stream().filter( i -> i.key.tableId == tableId && i.unique ).collect( Collectors.toList() );
        }
    }


    @Override
    public CatalogIndex getIndex( long tableId, String indexName ) throws UnknownIndexException {
        try {
            return indexes.values().stream()
                    .filter( i -> i.key.tableId == tableId && i.name.equals( indexName ) )
                    .findFirst()
                    .orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexException( tableId, indexName );
        }
    }


    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) {
        try {
            getIndex( tableId, indexName );
            return true;
        } catch ( UnknownIndexException e ) {
            return false;
        }
    }


    @Override
    public CatalogIndex getIndex( long indexId ) {
        try {
            return Objects.requireNonNull( indexes.get( indexId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexIdRuntimeException( indexId );
        }
    }


    @Override
    public List<CatalogIndex> getIndexes() {
        return new ArrayList<>( indexes.values() );
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
    public List<CatalogKey> getKeys() {
        return null;
    }


    @Override
    public List<CatalogKey> getTableKeys( long tableId ) {
        return null;
    }


    @Override
    public List<LogicalColumn> getColumns( long tableId ) {
        return null;
    }


    @Override
    public List<LogicalColumn> getColumns( @Nullable Pattern tableNamePattern, @Nullable Pattern columnNamePattern ) {
        List<LogicalTable> tables = getTables( tableNamePattern );
        if ( columnNamePattern == null ) {
            return tables.stream().flatMap( t -> t.columns.stream() ).collect( Collectors.toList() );
        }
        return tables.stream().flatMap( t -> t.columns.stream() ).filter( c -> c.name.matches( columnNamePattern.toRegex() ) ).collect( Collectors.toList() );
    }


    @Override
    public LogicalColumn getColumn( long columnId ) {
        return columns.get( columnId );
    }


    @Override
    public LogicalColumn getColumn( long tableId, String columnName ) throws UnknownColumnException {
        return null;
    }


    @Override
    public LogicalColumn getColumn( String schemaName, String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownTableException {
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
    public List<LogicalTable> getTablesForPeriodicProcessing() {
        return null;
    }


    @Override
    public void flagTableForDeletion( long tableId, boolean flag ) {

    }


    @Override
    public boolean isTableFlaggedForDeletion( long tableId ) {
        return false;
    }

}
