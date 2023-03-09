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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.util.Pair;

@Value
public class LogicalRelSnapshotImpl implements LogicalRelSnapshot {

    LogicalNamespace namespace;

    ImmutableMap<Long, LogicalTable> tables;

    ImmutableMap<String, LogicalTable> tableNames;

    ImmutableMap<Long, List<LogicalColumn>> tableColumns;
    ImmutableMap<Long, LogicalColumn> columns;

    ImmutableMap<String, LogicalColumn> columnNames;

    ImmutableMap<Long, CatalogKey> keys;

    ImmutableMap<Long, List<CatalogKey>> tableKeys;

    ImmutableMap<Long, CatalogIndex> index;

    ImmutableMap<Long, List<CatalogIndex>> keyToIndexes;

    ImmutableMap<Pair<Long, Long>, LogicalColumn> tableColumnIdColumn;

    ImmutableMap<Pair<String, String>, LogicalColumn> tableColumnNameColumn;

    ImmutableMap<Pair<Long, String>, LogicalColumn> tableIdColumnNameColumn;


    public LogicalRelSnapshotImpl( LogicalRelationalCatalog catalog ) {
        namespace = catalog.getLogicalNamespace();
        tables = ImmutableMap.copyOf( catalog.getTables() );
        tableNames = ImmutableMap.copyOf( tables.entrySet().stream().collect( Collectors.toMap( e -> namespace.caseSensitive ? e.getValue().name : e.getValue().name.toLowerCase(), Entry::getValue ) ) );
        columns = ImmutableMap.copyOf( catalog.getColumns() );
        columnNames = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( e -> namespace.caseSensitive ? e.getValue().name : e.getValue().name.toLowerCase(), Entry::getValue ) ) );

        Map<Long, List<LogicalColumn>> tableChildren = new HashMap<>();
        columns.forEach( ( k, v ) -> {
            if ( tableChildren.containsKey( v.tableId ) ) {
                tableChildren.get( v.tableId ).add( v );
            } else {
                tableChildren.put( v.tableId, new ArrayList<>( List.of( v ) ) );
            }
        } );
        this.tableColumns = ImmutableMap.copyOf( tableChildren );

        keys = catalog.getKeys();

        Map<Long, List<CatalogKey>> tableKeys = new HashMap<>();
        keys.forEach( ( k, v ) -> {
            if ( tableKeys.containsKey( v.tableId ) ) {
                tableKeys.get( v.tableId ).add( v );
            } else {
                tableKeys.put( v.tableId, new ArrayList<>( List.of( v ) ) );
            }
        } );

        this.tableKeys = ImmutableMap.copyOf( tableKeys );

        this.tableColumnIdColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().id ), Entry::getValue ) ) );
        this.tableColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( tables.get( c.getValue().tableId ).name, c.getValue().name ), Entry::getValue ) ) );
        this.tableIdColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().name ), Entry::getValue ) ) );

        this.index = catalog.getIndexes();

        Map<Long, List<CatalogIndex>> keyToIndexes = new HashMap<>();
        this.index.forEach( ( k, v ) -> {
            if ( keyToIndexes.containsKey( v.keyId ) ) {
                keyToIndexes.get( v.keyId ).add( v );
            } else {
                keyToIndexes.put( v.keyId, new ArrayList<>( List.of( v ) ) );
            }
        } );
        this.keyToIndexes = ImmutableMap.copyOf( keyToIndexes );

    }


    @Override
    public List<LogicalTable> getTables( @Nullable Pattern name ) {
        if ( name == null ) {
            return tables.values().asList();
        }
        return tables.values().stream().filter( t -> namespace.caseSensitive ? t.name.matches( name.toRegex() ) : t.name.toLowerCase().matches( (name.toRegex().toLowerCase()) ) ).collect( Collectors.toList() );
    }


    @Override
    public LogicalTable getTable( long tableId ) {
        return tables.get( tableId );
    }


    @Override
    public LogicalTable getTable( String tableName ) throws UnknownTableException {
        return tableNames.get( tableName );
    }


    @Override
    public List<CatalogKey> getKeys() {
        return keys.values().asList();
    }


    @Override
    public List<CatalogKey> getTableKeys( long tableId ) {
        return tableKeys.get( tableId );
    }


    @Override
    public List<LogicalColumn> getColumns( long tableId ) {
        return tableColumns.get( tableId );
    }


    @Override
    public List<LogicalColumn> getColumns( @Nullable Pattern tableName, @Nullable Pattern columnName ) {
        List<LogicalTable> tables = getTables( tableName );
        if ( columnName == null ) {
            return tables.stream().flatMap( t -> tableColumns.get( t.id ).stream() ).collect( Collectors.toList() );
        }

        return tables
                .stream()
                .flatMap( t -> tableColumns.get( t.id ).stream().filter(
                        c -> namespace.caseSensitive
                                ? c.name.matches( columnName.toRegex() )
                                : c.name.toLowerCase().matches( columnName.toLowerCase().toRegex() ) ) ).collect( Collectors.toList() );

    }


    @Override
    public LogicalColumn getColumn( long columnId ) {
        return columns.get( columnId );
    }


    @Override
    public LogicalColumn getColumn( long tableId, String columnName ) throws UnknownColumnException {
        return tableIdColumnNameColumn.get( Pair.of( tableId, columnName ) );
    }


    @Override
    public LogicalColumn getColumn( String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownTableException {
        return tableIdColumnNameColumn.get( Pair.of( tableName, columnName ) );
    }


    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) {
        return tableIdColumnNameColumn.containsKey( Pair.of( tableId, columnName ) );
    }


    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) {
        return (CatalogPrimaryKey) keys.get( key );
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
    public List<CatalogIndex> getIndexes() {
        return index.values().asList();
    }


    @Override
    public List<CatalogIndex> getIndexes( CatalogKey key ) {
        return keyToIndexes.get( key.id );
    }


    @Override
    public List<CatalogIndex> getForeignKeys( CatalogKey key ) {
        return keyToIndexes.get( key.id );
    }


    @Override
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) {
        return tableKeys.get( tableId ).stream().flatMap( k -> getIndexes( k ).stream() ).collect( Collectors.toList() );
    }


    @Override
    public CatalogIndex getIndex( long tableId, String indexName ) {
        return getIndex().values().stream().filter( i -> i.getKey().tableId == tableId && i.name.equals( indexName ) ).findFirst().orElse( null );
    }


    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) {
        return getIndex( tableId, indexName ) != null;
    }


    @Override
    public CatalogIndex getIndex( long indexId ) {
        return index.get( indexId );
    }


    @Override
    public LogicalTable getLogicalTable( long id ) {
        return tables.get( id );
    }


    @Override
    public LogicalTable getLogicalTable( String name ) {
        return tableNames.get( name );
    }


    @Override
    public LogicalColumn getLogicalColumn( long id ) {
        return columns.get( id );
    }


    @Override
    public boolean checkIfExistsEntity( String newName ) {
        return tableNames.containsKey( newName );
    }

}
