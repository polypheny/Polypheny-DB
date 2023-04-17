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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.util.Pair;

@Value
public class LogicalRelSnapshotImpl implements LogicalRelSnapshot {

    ImmutableMap<Long, LogicalNamespace> namespaces;

    ImmutableMap<String, LogicalNamespace> namespaceNames;

    ImmutableMap<Long, LogicalTable> tables;

    ImmutableMap<Long, LogicalView> views;

    ImmutableMap<Pair<Long, String>, LogicalTable> tableNames;

    ImmutableMap<Long, TreeSet<LogicalColumn>> tableColumns;
    ImmutableMap<Long, LogicalColumn> columns;

    ImmutableMap<Pair<Long, String>, LogicalColumn> columnNames;

    ImmutableMap<Long, LogicalKey> keys;

    ImmutableMap<Long, List<LogicalKey>> tableKeys;
    ImmutableMap<long[], LogicalKey> columnsKey;

    ImmutableMap<Long, LogicalIndex> index;

    ImmutableMap<Long, CatalogConstraint> constraints;

    ImmutableMap<Long, LogicalForeignKey> foreignKeys;
    ImmutableMap<Long, LogicalPrimaryKey> primaryKeys;

    ImmutableMap<Long, List<LogicalIndex>> keyToIndexes;

    ImmutableMap<Pair<Long, Long>, LogicalColumn> tableColumnIdColumn;

    ImmutableMap<Pair<String, String>, LogicalColumn> tableColumnNameColumn;

    ImmutableMap<Pair<Long, String>, LogicalColumn> tableIdColumnNameColumn;
    ImmutableMap<Long, List<CatalogConstraint>> tableConstraints;
    ImmutableMap<Long, List<LogicalForeignKey>> tableForeignKeys;
    ImmutableMap<Long, AlgNode> nodes;
    ImmutableMap<Long, List<LogicalView>> connectedViews;


    public LogicalRelSnapshotImpl( Map<Long, LogicalRelationalCatalog> catalogs ) {
        this.namespaces = ImmutableMap.copyOf( catalogs.values().stream().map( LogicalRelationalCatalog::getLogicalNamespace ).collect( Collectors.toMap( n -> n.id, n -> n ) ) );
        this.namespaceNames = ImmutableMap.copyOf( namespaces.values().stream().collect( Collectors.toMap( n -> n.name, n -> n ) ) );

        this.tables = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getTables().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );
        this.tableNames = ImmutableMap.copyOf( tables.entrySet().stream().collect( Collectors.toMap( e -> Pair.of( e.getValue().namespaceId, getAdjustedName( e.getValue().namespaceId, e.getValue().name ) ), Entry::getValue ) ) );

        this.columns = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getColumns().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );
        this.columnNames = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( e -> namespaces.get( e.getValue().namespaceId ).caseSensitive ? Pair.of( e.getValue().tableId, e.getValue().name ) : Pair.of( e.getValue().tableId, e.getValue().name.toLowerCase() ), Entry::getValue ) ) );

        //// tables

        this.tableColumns = buildTableColumns();

        this.tableColumnIdColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().id ), Entry::getValue ) ) );
        this.tableColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( tables.get( c.getValue().tableId ).name, c.getValue().name ), Entry::getValue ) ) );
        this.tableIdColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().name ), Entry::getValue ) ) );

        //// KEYS

        this.keys = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getKeys().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        this.tableKeys = buildTableKeys();

        this.columnsKey = buildColumnsKey();

        this.index = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getIndexes().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        this.keyToIndexes = buildKeyToIndexes();

        this.foreignKeys = ImmutableMap.copyOf( keys.entrySet().stream().filter( f -> f.getValue() instanceof LogicalForeignKey ).collect( Collectors.toMap( Entry::getKey, e -> (LogicalForeignKey) e.getValue() ) ) );

        this.tableForeignKeys = buildTableForeignKeys();

        this.primaryKeys = ImmutableMap.copyOf( keys.entrySet().stream().filter( f -> f.getValue() instanceof LogicalPrimaryKey ).collect( Collectors.toMap( Entry::getKey, e -> (LogicalPrimaryKey) e.getValue() ) ) );

        //// CONSTRAINTS

        this.constraints = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getConstraints().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        this.tableConstraints = buildTableConstraints();

        /// ALGNODES e.g. views and materializedViews
        this.nodes = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getNodes().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );

        this.views = buildViews();

        this.connectedViews = buildConnectedViews();

    }


    private ImmutableMap<Long, LogicalView> buildViews() {
        return ImmutableMap.copyOf( tables
                .values()
                .stream()
                .filter( t -> t.unwrap( LogicalView.class ) != null )
                .map( t -> t.unwrap( LogicalView.class ) )
                .collect( Collectors.toMap( e -> e.id, e -> e ) ) );
    }


    private ImmutableMap<Long, TreeSet<LogicalColumn>> buildTableColumns() {
        Map<Long, TreeSet<LogicalColumn>> map = new HashMap<>();
        columns.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.tableId ) ) {
                map.put( v.tableId, new TreeSet<>( Comparator.comparingInt( a -> a.position ) ) );
            }
            map.get( v.tableId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    private ImmutableMap<Long, List<LogicalIndex>> buildKeyToIndexes() {
        Map<Long, List<LogicalIndex>> map = new HashMap<>();
        this.index.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.keyId ) ) {
                map.put( v.keyId, new ArrayList<>() );
            }
            map.get( v.keyId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    private ImmutableMap<Long, List<CatalogConstraint>> buildTableConstraints() {
        Map<Long, List<CatalogConstraint>> map = new HashMap<>();
        constraints.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.key.tableId ) ) {
                map.put( v.key.tableId, new ArrayList<>() );
            }
            map.get( v.key.tableId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    private ImmutableMap<Long, List<LogicalForeignKey>> buildTableForeignKeys() {
        Map<Long, List<LogicalForeignKey>> map = new HashMap<>();
        foreignKeys.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.tableId ) ) {
                map.put( v.tableId, new ArrayList<>() );
            }
            map.get( v.tableId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<long[], LogicalKey> buildColumnsKey() {
        Map<long[], LogicalKey> map = keys.entrySet().stream().collect( Collectors.toMap( e -> e.getValue().columnIds.stream().mapToLong( c -> c ).toArray(), Entry::getValue ) );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<LogicalKey>> buildTableKeys() {
        Map<Long, List<LogicalKey>> tableKeys = new HashMap<>();
        keys.forEach( ( k, v ) -> {
            if ( !tableKeys.containsKey( v.tableId ) ) {
                tableKeys.put( v.tableId, new ArrayList<>() );
            }
            tableKeys.get( v.tableId ).add( v );
        } );
        return ImmutableMap.copyOf( tableKeys );
    }


    private ImmutableMap<Long, List<LogicalView>> buildConnectedViews() {
        Map<Long, List<LogicalView>> map = new HashMap<>();

        for ( LogicalView view : this.views.values() ) {
            for ( long entityId : view.underlyingTables.keySet() ) {
                if ( !map.containsKey( entityId ) ) {
                    map.put( entityId, new ArrayList<>() );
                }
                map.get( entityId ).add( view );
            }
        }
        // add tables which are not connected
        for ( long id : this.tables.keySet() ) {
            if ( !map.containsKey( id ) ) {
                map.put( id, new ArrayList<>() );
            }
        }

        return ImmutableMap.copyOf( map );
    }


    public String getAdjustedName( long namespaceId, String entityName ) {
        return namespaces.get( namespaceId ).caseSensitive ? entityName : entityName.toLowerCase();
    }


    @Override
    public List<LogicalTable> getTables( @Nullable Pattern namespaceName, Pattern name ) {
        List<Long> namespaceIds = getNamespaces( namespaceName ).stream().map( n -> n.id ).collect( Collectors.toList() );

        List<LogicalTable> tables = this.tables.values().asList();
        if ( name != null ) {
            tables = tables.stream()
                    .filter( t ->
                            this.namespaces.get( t.namespaceId ).caseSensitive
                                    ? t.name.matches( name.toRegex() )
                                    : t.name.matches( name.toRegex().toLowerCase() ) ).collect( Collectors.toList() );
        }
        return tables.stream().filter( t -> namespaceIds.contains( t.namespaceId ) ).collect( Collectors.toList() );
    }


    private List<LogicalNamespace> getNamespaces( @Nullable Pattern namespaceName ) {
        if ( namespaceName == null ) {
            return this.namespaces.values().asList();
        }
        return this.namespaces.values().stream().filter( n -> n.caseSensitive ? n.name.matches( namespaceName.toRegex() ) : n.name.matches( namespaceName.toRegex().toLowerCase() ) ).collect( Collectors.toList() );

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
    public List<LogicalKey> getKeys() {
        return keys.values().asList();
    }


    @Override
    public List<LogicalKey> getTableKeys( long tableId ) {
        return tableKeys.get( tableId );
    }


    @Override
    public List<LogicalColumn> getColumns( long tableId ) {
        return List.copyOf( tableColumns.get( tableId ) );
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
    public LogicalColumn getColumn( String tableName, String columnName ) {
        return tableIdColumnNameColumn.get( Pair.of( tableName, columnName ) );
    }


    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) {
        return tableIdColumnNameColumn.containsKey( Pair.of( tableId, columnName ) );
    }


    @Override
    public LogicalPrimaryKey getPrimaryKey( long key ) {
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
    public List<LogicalForeignKey> getForeignKeys( long tableId ) {
        return tableKeys.get( tableId ).stream().filter( k -> isForeignKey( k.id ) ).map( f -> (LogicalForeignKey) f ).collect( Collectors.toList() );
    }


    @Override
    public List<LogicalForeignKey> getExportedKeys( long tableId ) {
        return foreignKeys.values().stream().filter( k -> k.referencedKeyTableId == tableId ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) {
        List<Long> keysOfTable = getTableKeys( tableId ).stream().map( t -> t.id ).collect( Collectors.toList() );
        return constraints.values().stream().filter( c -> keysOfTable.contains( c.keyId ) ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogConstraint> getConstraints( LogicalKey key ) {
        return constraints.values().stream().filter( c -> c.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) {
        return tableConstraints.get( tableId ).stream().filter( c -> c.name.equals( constraintName ) ).findFirst().orElse( null );
    }


    @Override
    public LogicalForeignKey getForeignKey( long tableId, String foreignKeyName ) {
        return tableForeignKeys.get( tableId ).stream().filter( e -> e.name.equals( foreignKeyName ) ).findFirst().orElse( null );
    }


    @Override
    public List<LogicalIndex> getIndexes() {
        return index.values().asList();
    }


    @Override
    public List<LogicalIndex> getIndexes( LogicalKey key ) {
        return keyToIndexes.get( key.id );
    }


    @Override
    public List<LogicalIndex> getForeignKeys( LogicalKey key ) {
        return keyToIndexes.get( key.id );
    }


    @Override
    public List<LogicalIndex> getIndexes( long tableId, boolean onlyUnique ) {
        return index.values().stream().filter( i -> i.key.tableId == tableId && (!onlyUnique || i.unique) ).collect( Collectors.toList() );
    }


    @Override
    public LogicalIndex getIndex( long tableId, String indexName ) {
        return getIndex().values().stream().filter( i -> i.getKey().tableId == tableId && i.name.equals( indexName ) ).findFirst().orElse( null );
    }


    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) {
        return getIndex( tableId, indexName ) != null;
    }


    @Override
    public LogicalIndex getIndex( long indexId ) {
        return index.get( indexId );
    }


    @Override
    public LogicalTable getTable( long namespaceId, String name ) {
        String adjustedName = name;
        if ( !namespaces.get( namespaceId ).caseSensitive ) {
            adjustedName = name.toLowerCase();
        }
        return tableNames.get( Pair.of( namespaceId, adjustedName ) );
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


    @Override
    public AlgNode getNodeInfo( long id ) {
        return nodes.get( id );
    }


    @Override
    public List<LogicalView> getConnectedViews( long id ) {
        return connectedViews.get( id );
    }


    @Override
    public LogicalKey getKeys( long[] columnIds ) {
        return columnsKey.get( columnIds );
    }


    @Override
    public LogicalKey getKey( long id ) {
        return keys.get( id );
    }


}
