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

    ImmutableMap<Long, LogicalNamespace> namespaces;

    ImmutableMap<String, LogicalNamespace> namespaceNames;

    ImmutableMap<Long, LogicalTable> tables;

    ImmutableMap<Pair<Long, String>, LogicalTable> tableNames;

    ImmutableMap<Long, List<LogicalColumn>> tableColumns;
    ImmutableMap<Long, LogicalColumn> columns;

    ImmutableMap<Pair<Long, String>, LogicalColumn> columnNames;

    ImmutableMap<Long, CatalogKey> keys;

    ImmutableMap<Long, List<CatalogKey>> tableKeys;

    ImmutableMap<Long, CatalogIndex> index;

    ImmutableMap<Long, CatalogConstraint> constraints;

    ImmutableMap<Long, CatalogForeignKey> foreignKeys;
    ImmutableMap<Long, CatalogPrimaryKey> primaryKeys;

    ImmutableMap<Long, List<CatalogIndex>> keyToIndexes;

    ImmutableMap<Pair<Long, Long>, LogicalColumn> tableColumnIdColumn;

    ImmutableMap<Pair<String, String>, LogicalColumn> tableColumnNameColumn;

    ImmutableMap<Pair<Long, String>, LogicalColumn> tableIdColumnNameColumn;
    ImmutableMap<Long, List<CatalogConstraint>> tableConstraints;
    ImmutableMap<Long, List<CatalogForeignKey>> tableForeignKeys;


    public LogicalRelSnapshotImpl( Map<Long, LogicalRelationalCatalog> catalogs ) {
        namespaces = ImmutableMap.copyOf( catalogs.values().stream().map( LogicalRelationalCatalog::getLogicalNamespace ).collect( Collectors.toMap( n -> n.id, n -> n ) ) );
        namespaceNames = ImmutableMap.copyOf( namespaces.values().stream().collect( Collectors.toMap( n -> n.name, n -> n ) ) );

        tables = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getTables().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );
        tableNames = ImmutableMap.copyOf( tables.entrySet().stream().collect( Collectors.toMap( e -> Pair.of( e.getValue().namespaceId, namespaces.get( e.getValue().namespaceId ).caseSensitive ? e.getValue().name : e.getValue().name.toLowerCase() ), Entry::getValue ) ) );

        columns = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getColumns().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );
        columnNames = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( e -> namespaces.get( e.getValue().namespaceId ).caseSensitive ? Pair.of( e.getValue().tableId, e.getValue().name ) : Pair.of( e.getValue().tableId, e.getValue().name.toLowerCase() ), Entry::getValue ) ) );

        //// tables

        Map<Long, List<LogicalColumn>> tableChildren = new HashMap<>();
        columns.forEach( ( k, v ) -> {
            if ( !tableChildren.containsKey( v.tableId ) ) {
                tableChildren.put( v.tableId, new ArrayList<>() );
            }
            tableChildren.get( v.tableId ).add( v );
        } );
        this.tableColumns = ImmutableMap.copyOf( tableChildren );

        this.tableColumnIdColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().id ), Entry::getValue ) ) );
        this.tableColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( tables.get( c.getValue().tableId ).name, c.getValue().name ), Entry::getValue ) ) );
        this.tableIdColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().name ), Entry::getValue ) ) );

        //// KEYS

        keys = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getKeys().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        Map<Long, List<CatalogKey>> tableKeys = new HashMap<>();
        keys.forEach( ( k, v ) -> {
            if ( !tableKeys.containsKey( v.tableId ) ) {
                tableKeys.put( v.tableId, new ArrayList<>() );
            }
            tableKeys.get( v.tableId ).add( v );
        } );

        this.tableKeys = ImmutableMap.copyOf( tableKeys );

        this.index = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getIndexes().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        Map<Long, List<CatalogIndex>> keyToIndexes = new HashMap<>();
        this.index.forEach( ( k, v ) -> {
            if ( !keyToIndexes.containsKey( v.keyId ) ) {
                keyToIndexes.put( v.keyId, new ArrayList<>() );
            }
            keyToIndexes.get( v.keyId ).add( v );
        } );
        this.keyToIndexes = ImmutableMap.copyOf( keyToIndexes );

        this.foreignKeys = ImmutableMap.copyOf( keys.entrySet().stream().filter( f -> f.getValue() instanceof CatalogForeignKey ).collect( Collectors.toMap( Entry::getKey, e -> (CatalogForeignKey) e.getValue() ) ) );

        HashMap<Long, List<CatalogForeignKey>> tableForeignKeys = new HashMap<>();
        foreignKeys.forEach( ( k, v ) -> {
            if ( !tableForeignKeys.containsKey( v.tableId ) ) {
                tableForeignKeys.put( v.tableId, new ArrayList<>() );
            }
            tableForeignKeys.get( v.tableId ).add( v );
        } );
        this.tableForeignKeys = ImmutableMap.copyOf( tableForeignKeys );

        this.primaryKeys = ImmutableMap.copyOf( keys.entrySet().stream().filter( f -> f.getValue() instanceof CatalogPrimaryKey ).collect( Collectors.toMap( Entry::getKey, e -> (CatalogPrimaryKey) e.getValue() ) ) );

        //// CONSTRAINTS

        this.constraints = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getConstraints().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        HashMap<Long, List<CatalogConstraint>> tableConstraints = new HashMap<>();
        constraints.forEach( ( k, v ) -> {
            if ( !tableConstraints.containsKey( v.key.tableId ) ) {
                tableConstraints.put( v.key.tableId, new ArrayList<>() );
            }
            tableConstraints.get( v.key.tableId ).add( v );
        } );
        this.tableConstraints = ImmutableMap.copyOf( tableConstraints );
    }


    @Override
    public List<LogicalTable> getTables( @javax.annotation.Nullable Pattern namespace, Pattern name ) {
        if ( name == null ) {
            return tables.values().asList();
        }
        return tables.values().stream().filter( t -> namespaces.get( t.namespaceId ).caseSensitive ? t.name.matches( name.toRegex() ) : t.name.toLowerCase().matches( (name.toRegex().toLowerCase()) ) ).collect( Collectors.toList() );
    }


    @Override
    public List<LogicalTable> getTables( long namespaceId, @Nullable Pattern name ) {
        return tableNames.values().stream().filter( e -> e.name.matches( name.toRegex() ) || e.namespaceId == namespaceId ).collect( Collectors.toList() );
    }


    @Override
    public List<LogicalTable> getTables( @Nullable String namespace, @Nullable String name ) {
        return null;
    }


    @Override
    public LogicalTable getTable( long tableId ) {
        return tables.get( tableId );
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
    public List<LogicalColumn> getColumns( Pattern tableName, Pattern columnName ) {
        List<LogicalTable> tables = getTables( null, tableName );
        if ( columnName == null ) {
            return tables.stream().flatMap( t -> tableColumns.get( t.id ).stream() ).collect( Collectors.toList() );
        }

        return tables
                .stream()
                .flatMap( t -> tableColumns.get( t.id ).stream().filter(
                        c -> namespaces.get( t.namespaceId ).caseSensitive
                                ? c.name.matches( columnName.toRegex() )
                                : c.name.toLowerCase().matches( columnName.toLowerCase().toRegex() ) ) ).collect( Collectors.toList() );

    }


    @Override
    public LogicalColumn getColumn( long tableId, String columnName ) {
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
        return primaryKeys.get( key );
    }


    @Override
    public boolean isPrimaryKey( long keyId ) {
        return primaryKeys.containsKey( keyId );
    }


    @Override
    public boolean isForeignKey( long keyId ) {
        return foreignKeys.containsKey( keyId );
    }


    @Override
    public boolean isIndex( long keyId ) {
        return index.containsKey( keyId );
    }


    @Override
    public boolean isConstraint( long keyId ) {
        return constraints.containsKey( keyId );
    }


    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) {
        return tableKeys.get( tableId ).stream().filter( k -> isForeignKey( k.id ) ).map( f -> (CatalogForeignKey) f ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) {
        return foreignKeys.values().stream().filter( k -> k.referencedKeyTableId == tableId ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) {
        List<Long> keysOfTable = getTableKeys( tableId ).stream().map( t -> t.id ).collect( Collectors.toList() );
        return constraints.values().stream().filter( c -> keysOfTable.contains( c.keyId ) ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogConstraint> getConstraints( CatalogKey key ) {
        return constraints.values().stream().filter( c -> c.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) throws UnknownConstraintException {
        return tableConstraints.get( tableId ).stream().filter( c -> c.name.equals( constraintName ) ).findFirst().orElse( null );
    }


    @Override
    public CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws UnknownForeignKeyException {
        return tableForeignKeys.get( tableId ).stream().filter( e -> e.name.equals( foreignKeyName ) ).findFirst().orElse( null );
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
    public LogicalTable getTable( long namespaceId, String name ) {
        String adjustedName = name;
        if ( !namespaces.get( namespaceId ).caseSensitive ) {
            adjustedName = name.toLowerCase();
        }
        return tableNames.get( adjustedName );
    }


    @Override
    public LogicalTable getTable( String namespaceName, String tableName ) {
        LogicalNamespace namespace = namespaceNames.get( namespaceName );
        return tableNames.get( Pair.of( namespace.id, namespace.caseSensitive ? tableName : tableName.toLowerCase() ) );
    }


    @Override
    public LogicalColumn getColumn( long id ) {
        return columns.get( id );
    }


    @Override
    public boolean checkIfExistsEntity( String newName ) {
        return tableNames.containsKey( newName );
    }

}
