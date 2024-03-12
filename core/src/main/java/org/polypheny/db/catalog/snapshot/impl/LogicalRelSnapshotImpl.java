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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.util.Pair;

@Value
@Slf4j
@EqualsAndHashCode
public class LogicalRelSnapshotImpl implements LogicalRelSnapshot {

    ImmutableMap<Long, LogicalNamespace> namespaces;

    ImmutableMap<String, LogicalNamespace> namespaceNames;

    ImmutableMap<Long, Boolean> namespaceCaseSensitive;

    ImmutableMap<Long, LogicalTable> tables;

    ImmutableMap<Long, LogicalView> views;

    ImmutableMap<Pair<Long, String>, LogicalTable> tableNames;

    ImmutableMap<Long, List<LogicalTable>> tablesNamespace;

    ImmutableMap<Long, TreeSet<LogicalColumn>> tableColumns;

    ImmutableMap<Long, LogicalColumn> columns;

    ImmutableMap<Pair<Long, String>, LogicalColumn> columnNames;

    ImmutableMap<Long, LogicalKey> keys;

    ImmutableMap<Long, List<LogicalKey>> tableKeys;

    ImmutableMap<long[], LogicalKey> columnsKeys;

    ImmutableMap<Long, LogicalIndex> index;

    ImmutableMap<Long, LogicalConstraint> constraints;

    ImmutableMap<Long, LogicalForeignKey> foreignKeys;

    ImmutableMap<Long, LogicalPrimaryKey> primaryKeys;

    ImmutableMap<Long, List<LogicalIndex>> keyToIndexes;

    ImmutableMap<Pair<Long, Long>, LogicalColumn> tableColumnIdColumn;

    ImmutableMap<Pair<Long, Pair<String, String>>, LogicalColumn> tableColumnNameColumn;

    ImmutableMap<Pair<Long, String>, LogicalColumn> tableIdColumnNameColumn;

    ImmutableMap<Long, List<LogicalConstraint>> tableConstraints;

    ImmutableMap<Long, List<LogicalForeignKey>> tableForeignKeys;

    ImmutableMap<Long, AlgNode> nodes;

    ImmutableMap<Long, List<LogicalView>> connectedViews;


    public LogicalRelSnapshotImpl( Map<Long, LogicalRelationalCatalog> catalogs ) {
        this.namespaces = ImmutableMap.copyOf( catalogs.values().stream().map( LogicalRelationalCatalog::getLogicalNamespace ).collect( Collectors.toMap( n -> n.id, n -> n, getDuplicateError() ) ) );
        this.namespaceNames = ImmutableMap.copyOf( namespaces.values().stream().collect( Collectors.toMap( n -> n.name, n -> n, getDuplicateError() ) ) );
        this.namespaceCaseSensitive = buildNamespaceCasing();

        this.tables = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getTables().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue, getDuplicateError() ) ) );
        this.tableNames = ImmutableMap.copyOf( tables.entrySet().stream().collect( Collectors.toMap( e -> Pair.of( e.getValue().namespaceId, getAdjustedName( e.getValue().namespaceId, e.getValue().name ) ), Entry::getValue, getDuplicateError() ) ) );
        this.tablesNamespace = buildTablesNamespace();

        this.columns = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getColumns().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue, getDuplicateError() ) ) );
        this.columnNames = ImmutableMap.copyOf(
                columns.entrySet().stream().collect( Collectors.toMap( e -> Pair.of( e.getValue().tableId, getAdjustedName( e.getValue().namespaceId, e.getValue().name ) ), Entry::getValue, getDuplicateError() ) ) );

        //// TABLES

        this.tableColumns = buildTableColumns();

        this.tableColumnIdColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, c.getValue().id ), Entry::getValue, getDuplicateError() ) ) );
        this.tableColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().namespaceId, Pair.of( tables.get( c.getValue().tableId ).name, getAdjustedName( c.getValue().namespaceId, c.getValue().name ) ) ), Entry::getValue, getDuplicateError() ) ) );
        this.tableIdColumnNameColumn = ImmutableMap.copyOf( columns.entrySet().stream().collect( Collectors.toMap( c -> Pair.of( c.getValue().tableId, getAdjustedName( c.getValue().namespaceId, c.getValue().name ) ), Entry::getValue, getDuplicateError() ) ) );

        //// KEYS

        this.keys = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getKeys().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue, getDuplicateError() ) ) );

        this.tableKeys = buildTableKeys();

        this.columnsKeys = buildColumnsKey();

        this.index = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getIndexes().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue, getDuplicateError() ) ) );

        this.keyToIndexes = buildKeyToIndexes();

        this.foreignKeys = ImmutableMap.copyOf( keys.entrySet().stream().filter( f -> f.getValue() instanceof LogicalForeignKey ).collect( Collectors.toMap( Entry::getKey, e -> (LogicalForeignKey) e.getValue(), getDuplicateError() ) ) );

        this.tableForeignKeys = buildTableForeignKeys();

        this.primaryKeys = ImmutableMap.copyOf( keys.entrySet().stream().filter( f -> f.getValue() instanceof LogicalPrimaryKey ).collect( Collectors.toMap( Entry::getKey, e -> (LogicalPrimaryKey) e.getValue(), getDuplicateError() ) ) );

        //// CONSTRAINTS

        this.constraints = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getConstraints().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue, getDuplicateError() ) ) );

        this.tableConstraints = buildTableConstraints();

        /// ALGNODES e.g. views and materializedViews
        this.nodes = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getNodes().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue, getDuplicateError() ) ) );

        this.views = buildViews();

        this.connectedViews = buildConnectedViews();

    }


    @NotNull
    private static <D> BinaryOperator<D> getDuplicateError() {
        return ( a, b ) -> {
            throw new GenericRuntimeException( "Duplicates merging snapshot" );
        };
    }


    private ImmutableMap<Long, Boolean> buildNamespaceCasing() {
        return ImmutableMap.copyOf( namespaces.values().stream().collect( Collectors.toMap( n -> n.id, n -> n.caseSensitive, getDuplicateError() ) ) );
    }


    private ImmutableMap<Long, List<LogicalTable>> buildTablesNamespace() {
        Map<Long, List<LogicalTable>> map = new HashMap<>();
        namespaces.values().forEach( n -> map.put( n.id, new ArrayList<>() ) );
        tables.values().forEach( t -> map.get( t.namespaceId ).add( t ) );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, LogicalView> buildViews() {
        return ImmutableMap.copyOf( tables
                .values()
                .stream()
                .map( t -> t.unwrap( LogicalView.class ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( Collectors.toMap( e -> e.id, e -> e, getDuplicateError() ) ) );
    }


    private ImmutableMap<Long, TreeSet<LogicalColumn>> buildTableColumns() {
        Map<Long, TreeSet<LogicalColumn>> map = new HashMap<>();
        tables.values().forEach( t -> map.put( t.id, new TreeSet<>( ( a, b ) -> a.position == b.position ? (int) (a.id - b.id) : a.position - b.position ) ) ); // while this should not happen, we ensure consistency with this
        columns.forEach( ( k, v ) -> {
            map.get( v.tableId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    private ImmutableMap<Long, List<LogicalIndex>> buildKeyToIndexes() {
        Map<Long, List<LogicalIndex>> map = new HashMap<>();
        keys.values().forEach( k -> map.put( k.id, new ArrayList<>() ) );
        this.index.forEach( ( k, v ) -> map.get( v.keyId ).add( v ) );
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    private ImmutableMap<Long, List<LogicalConstraint>> buildTableConstraints() {
        Map<Long, List<LogicalConstraint>> map = new HashMap<>();

        tables.values().forEach( t -> map.put( t.id, new ArrayList<>() ) );
        constraints.forEach( ( k, v ) -> map.get( v.key.entityId ).add( v ) );
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    private ImmutableMap<Long, List<LogicalForeignKey>> buildTableForeignKeys() {
        Map<Long, List<LogicalForeignKey>> map = new HashMap<>();
        tables.values().forEach( t -> map.put( t.id, new ArrayList<>() ) );
        foreignKeys.forEach( ( k, v ) -> map.get( v.entityId ).add( v ) );
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<long[], LogicalKey> buildColumnsKey() {
        Map<long[], LogicalKey> map = keys.entrySet().stream().collect( Collectors.toMap( e -> e.getValue().fieldIds.stream().mapToLong( c -> c ).toArray(), Entry::getValue, getDuplicateError() ) );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<LogicalKey>> buildTableKeys() {
        Map<Long, List<LogicalKey>> tableKeys = new HashMap<>();
        for ( LogicalTable table : tables.values() ) {
            tableKeys.put( table.id, new ArrayList<>() );
        }
        keys.forEach( ( k, v ) -> tableKeys.get( v.entityId ).add( v ) );
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
        return Objects.requireNonNull( namespaces.get( namespaceId ) ).caseSensitive ? entityName : entityName.toLowerCase();
    }


    @Override
    public @NonNull List<LogicalTable> getTables( @Nullable Pattern namespaceName, Pattern name ) {
        List<Long> namespaceIds = getNamespaces( namespaceName ).stream().map( n -> n.id ).toList();

        List<LogicalTable> tables = this.tables.values().asList();
        if ( name != null ) {
            tables = tables.stream().filter( t -> this.namespaces.get( t.namespaceId ) != null )
                    .filter( t ->
                            Objects.requireNonNull( this.namespaces.get( t.namespaceId ) ).caseSensitive
                                    ? t.name.matches( name.toRegex() )
                                    : t.name.matches( name.toRegex().toLowerCase() ) ).toList();
        }
        return Optional.of( tables.stream().filter( t -> namespaceIds.contains( t.namespaceId ) ).toList() ).orElse( List.of() );
    }


    private List<LogicalNamespace> getNamespaces( @Nullable Pattern namespaceName ) {
        if ( namespaceName == null ) {
            return this.namespaces.values().asList();
        }
        return this.namespaces.values().stream().filter( n -> n.caseSensitive ? n.name.matches( namespaceName.toRegex() ) : n.name.toLowerCase().matches( namespaceName.toRegex().toLowerCase() ) ).toList();

    }


    @Override
    public @NotNull List<LogicalTable> getTables( long namespaceId, @Nullable Pattern name ) {

        LogicalNamespace namespace = namespaces.get( namespaceId );
        if ( namespace == null ) {
            return List.of();
        }

        return Optional.of( Objects.requireNonNull( tablesNamespace.get( namespaceId ) ).stream().filter( e -> (name == null || (namespace.caseSensitive
                ? e.name.toLowerCase().matches( name.toRegex() )
                : e.name.toLowerCase().matches( name.toRegex().toLowerCase() ))) ).toList() ).orElse( List.of() );
    }


    @Override
    public @NonNull Optional<LogicalTable> getTables( @Nullable String namespaceName, @NonNull String name ) {
        LogicalNamespace namespace = namespaceNames.get( namespaceName );

        assert namespace != null;
        return Optional.ofNullable( tableNames.get( Pair.of( namespace.id, (namespace.caseSensitive ? name : name.toLowerCase()) ) ) );
    }


    @Override
    public @NonNull List<LogicalTable> getTablesFromNamespace( long namespace ) {
        return Optional.ofNullable( tablesNamespace.get( namespace ) ).orElse( List.of() );
    }


    @Override
    public @NonNull Optional<LogicalTable> getTable( long tableId ) {
        return Optional.ofNullable( tables.get( tableId ) );
    }


    @Override
    public @NonNull List<LogicalConstraint> getConstraints() {
        return constraints.values().asList();
    }


    @Override
    public @NonNull List<LogicalKey> getKeys() {
        return keys.values().asList();
    }


    @Override
    public @NotNull List<LogicalPrimaryKey> getPrimaryKeys() {
        return primaryKeys.values().asList();
    }


    @Override
    public @NonNull List<LogicalKey> getTableKeys( long tableId ) {
        return Optional.ofNullable( tableKeys.get( tableId ) ).orElse( List.of() );
    }


    @Override
    public @NonNull List<LogicalColumn> getColumns( long tableId ) {
        return Optional.ofNullable( tableColumns.get( tableId ) ).map( List::copyOf ).orElse( List.of() );
    }


    @Override
    public @NonNull List<LogicalColumn> getColumns( Pattern tableName, Pattern columnName ) {
        List<LogicalTable> tables = getTables( null, tableName );
        if ( columnName == null ) {
            return tables.stream().flatMap( t -> Objects.requireNonNull( tableColumns.get( t.id ) ).stream() ).toList();
        }

        return tables
                .stream()
                .flatMap( t -> tableColumns.get( t.id ).stream().filter(
                        c -> Objects.requireNonNull( namespaces.get( t.namespaceId ) ).caseSensitive
                                ? c.name.matches( columnName.toRegex() )
                                : c.name.toLowerCase().matches( columnName.toLowerCase().toRegex() ) ) ).toList();

    }


    @Override
    public @NotNull Optional<LogicalColumn> getColumn( long tableId, String columnName ) {
        return getTable( tableId ).map( t -> namespaces.get( t.namespaceId ) ).map( n -> tableIdColumnNameColumn.get( Pair.of( tableId, n.caseSensitive ? columnName : columnName.toLowerCase() ) ) );
    }


    @Override
    public @NotNull Optional<LogicalColumn> getColumn( long namespace, String tableName, String columnName ) {
        Optional<LogicalTable> optTable = getTable( namespace, tableName );
        return optTable.map( logicalTable -> tableIdColumnNameColumn.get( Pair.of( logicalTable.id, columnName ) ) );

    }


    @Override
    public @NonNull Optional<LogicalPrimaryKey> getPrimaryKey( long key ) {
        return Optional.ofNullable( primaryKeys.get( key ) );
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
    public @NotNull List<LogicalForeignKey> getForeignKeys( long tableId ) {
        return foreignKeys.values().stream().filter( k -> k.entityId == tableId ).collect( Collectors.toList() );
    }


    @Override
    public @NonNull List<LogicalForeignKey> getExportedKeys( long tableId ) {
        return foreignKeys.values().stream().filter( k -> k.referencedKeyEntityId == tableId ).collect( Collectors.toList() );
    }


    @Override
    public @NonNull List<LogicalConstraint> getConstraints( long tableId ) {
        List<Long> keysOfTable = getTableKeys( tableId ).stream().map( t -> t.id ).toList();
        return constraints.values().stream().filter( c -> keysOfTable.contains( c.keyId ) ).collect( Collectors.toList() );
    }


    @Override
    public @NotNull List<LogicalConstraint> getConstraints( LogicalKey key ) {
        return constraints.values().stream().filter( c -> c.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public @NonNull Optional<LogicalConstraint> getConstraint( long tableId, String constraintName ) {
        return tableConstraints.get( tableId ).stream().filter( c -> c.name.equals( constraintName ) ).findFirst();
    }


    @Override
    public @NonNull Optional<LogicalForeignKey> getForeignKey( long tableId, String foreignKeyName ) {
        return tableForeignKeys.get( tableId ).stream().filter( e -> e.name.equals( foreignKeyName ) ).findFirst();
    }


    @Override
    public List<LogicalIndex> getIndexes() {
        return index.values().asList();
    }


    @Override
    public @NonNull List<LogicalIndex> getIndexes( LogicalKey key ) {
        return Optional.ofNullable( keyToIndexes.get( key.id ) ).orElse( List.of() );
    }


    @Override
    public @NotNull List<LogicalIndex> getForeignKeys( LogicalKey key ) {
        return Optional.ofNullable( keyToIndexes.get( key.id ) ).orElse( List.of() );
    }


    @Override
    public @NonNull List<LogicalIndex> getIndexes( long tableId, boolean onlyUnique ) {
        return index.values().stream().filter( i -> i.key.entityId == tableId && (!onlyUnique || i.unique) ).toList();
    }


    @Override
    public @NotNull Optional<LogicalIndex> getIndex( long tableId, String indexName ) {
        return getIndex().values().stream().filter( i -> i.getKey().entityId == tableId && i.name.equals( indexName ) ).findFirst();
    }


    @Override
    public @NonNull Optional<LogicalIndex> getIndex( long indexId ) {
        return Optional.ofNullable( index.get( indexId ) );
    }


    @Override
    public @NonNull Optional<LogicalTable> getTable( long namespaceId, String name ) {
        String adjustedName = name;
        if ( namespaces.containsKey( namespaceId ) && !namespaces.get( namespaceId ).caseSensitive ) {
            adjustedName = name.toLowerCase();
        }
        return Optional.ofNullable( tableNames.get( Pair.of( namespaceId, adjustedName ) ) );
    }


    @Override
    public @NonNull Optional<LogicalTable> getTable( String namespaceName, String tableName ) {
        LogicalNamespace namespace = namespaceNames.get( namespaceName.toLowerCase() );
        if ( namespace == null ) {
            return Optional.empty();
        }
        return Optional.ofNullable( tableNames.get( Pair.of( namespace.id, namespace.caseSensitive ? tableName : tableName.toLowerCase() ) ) );
    }


    @Override
    public @NonNull Optional<LogicalColumn> getColumn( long id ) {
        return Optional.ofNullable( columns.get( id ) );
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
    public @NotNull Optional<LogicalKey> getKeys( long[] columnIds ) {
        return Optional.ofNullable( columnsKeys.get( columnIds ) );
    }


    @Override
    public @NonNull Optional<LogicalKey> getKey( long id ) {
        return Optional.ofNullable( keys.get( id ) );
    }


}
