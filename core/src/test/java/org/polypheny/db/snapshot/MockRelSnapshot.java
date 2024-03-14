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

package org.polypheny.db.snapshot;

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.snapshot.MockSnapshot.MockTable;

public class MockRelSnapshot implements LogicalRelSnapshot {

    private final MockSnapshot snapshot;


    /**
     * Creates a MockRelSnapshot, which provides pre-mocked entities to a callee.
     * The MockRelSnapshot does ignore casing when comparing entity names.
     * Be aware that callers still might rely on case-sensitivity.
     */
    public MockRelSnapshot( MockSnapshot snapshot ) {
        this.snapshot = snapshot;
    }


    @Override
    public @NonNull List<LogicalTable> getTables( @Nullable Pattern namespace, @Nullable Pattern name ) {
        return getTables( 0L, name );
    }


    @Override
    public @NonNull List<LogicalTable> getTables( long namespaceId, @Nullable Pattern name ) {
        assert namespaceId == 0L; // only support one namespace for now in this mock
        return snapshot.tables.stream().filter( t -> name == null || name.pattern.matches( t.name ) ).map( MockTable::toLogical ).toList();
    }


    @Override
    public @NonNull Optional<LogicalTable> getTables( @Nullable String namespace, @NonNull String name ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalTable> getTablesFromNamespace( long namespace ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalTable> getTable( long namespaceId, String tableName ) {
        assert namespaceId == 0L; // only support one namespace for now in this mock
        return getTable( null, tableName );
    }


    @Override
    public @NonNull Optional<LogicalTable> getTable( String namespaceName, String tableName ) {
        Optional<MockTable> table = snapshot.tables.stream().filter( t -> t.name.equalsIgnoreCase( tableName ) ).findFirst();
        return table.map( MockTable::toLogical );
    }


    @Override
    public @NonNull List<LogicalKey> getKeys() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalKey> getTableKeys( long tableId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalColumn> getColumns( long tableId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalColumn> getColumns( @Nullable Pattern tableName, @Nullable Pattern columnName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalColumn> getColumn( long columnId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalColumn> getColumn( long tableId, String columnName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalColumn> getColumn( long namespace, String tableName, String columnName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalPrimaryKey> getPrimaryKey( long key ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isPrimaryKey( long keyId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isForeignKey( long keyId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isIndex( long keyId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isConstraint( long keyId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalForeignKey> getForeignKeys( long tableId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalForeignKey> getExportedKeys( long tableId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalConstraint> getConstraints( long tableId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalConstraint> getConstraints( LogicalKey key ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalConstraint> getConstraint( long tableId, String constraintName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalForeignKey> getForeignKey( long tableId, String foreignKeyName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<LogicalIndex> getIndexes() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalIndex> getIndexes( LogicalKey key ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalIndex> getForeignKeys( LogicalKey key ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull List<LogicalIndex> getIndexes( long tableId, boolean onlyUnique ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalIndex> getIndex( long tableId, String indexName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalIndex> getIndex( long indexId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NonNull Optional<LogicalTable> getTable( long id ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public AlgNode getNodeInfo( long id ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<LogicalView> getConnectedViews( long id ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalKey> getKeys( long[] columnIds ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalKey> getKey( long id ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull List<LogicalConstraint> getConstraints() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull List<LogicalPrimaryKey> getPrimaryKeys() {
        throw new UnsupportedOperationException();
    }

}
