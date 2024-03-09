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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalQueryInterface;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;
import org.polypheny.db.type.PolyType;

public class MockSnapshot implements Snapshot {

    List<MockTable> tables = new ArrayList<>();

    List<MockLogicalNamespace> namespaces = new ArrayList<>();


    @Override
    public long id() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull List<LogicalNamespace> getNamespaces( @Nullable Pattern name ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalNamespace> getNamespace( long id ) {
        assert id == 0L; // only support one namespace for now in this mock
        return namespaces.stream().filter( ns -> ns.id == id ).map( ns -> (LogicalNamespace) ns ).findFirst();
    }


    @Override
    public @NotNull Optional<LogicalNamespace> getNamespace( String name ) {
        return namespaces.stream().filter( ns -> ns.name.equals( name ) ).map( ns -> (LogicalNamespace) ns ).findFirst();
    }


    @Override
    public @NotNull Optional<LogicalUser> getUser( String name ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalUser> getUser( long id ) {
        return Optional.of( new LogicalUser( id, "pa", "" ) );
    }


    @Override
    public List<LogicalAdapter> getAdapters() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalAdapter> getAdapter( String uniqueName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalAdapter> getAdapter( long id ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull List<LogicalQueryInterface> getQueryInterfaces() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalQueryInterface> getQueryInterface( String uniqueName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<QueryInterfaceTemplate> getInterfaceTemplate( String name ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<LogicalTable> getTablesForPeriodicProcessing() {
        throw new UnsupportedOperationException();
    }


    @Override
    public Optional<AdapterTemplate> getAdapterTemplate( long templateId ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull List<AdapterTemplate> getAdapterTemplates() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<? extends LogicalEntity> getLogicalEntity( long id ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<AdapterTemplate> getAdapterTemplate( String name, AdapterType adapterType ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<AdapterTemplate> getAdapterTemplates( AdapterType adapterType ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<QueryInterfaceTemplate> getInterfaceTemplates() {
        throw new UnsupportedOperationException();
    }


    @Override
    public LogicalRelSnapshot rel() {
        return new MockRelSnapshot( this );
    }


    @Override
    public LogicalGraphSnapshot graph() {
        throw new UnsupportedOperationException();
    }


    @Override
    public LogicalDocSnapshot doc() {
        throw new UnsupportedOperationException();
    }


    @Override
    public AllocSnapshot alloc() {
        throw new UnsupportedOperationException();
    }


    @Override
    public @NotNull Optional<LogicalEntity> getLogicalEntity( long namespaceId, String entity ) {
        throw new UnsupportedOperationException();
    }


    public void mock( MockLogicalNamespace namespace ) {
        this.namespaces.add( namespace );
    }


    public void mock( MockTable table ) {
        this.tables.add( table );
    }


    @Value
    public static class MockColumnInfo {

        public String name;
        public boolean nullable;
        public PolyType type;
        @Nullable
        public Integer precision;


        public MockColumnInfo( String name, boolean nullable, PolyType type, @Nullable Integer precision ) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.precision = precision;
        }


        public MockColumnInfo( String name, boolean nullable, PolyType type ) {
            this( name, nullable, type, null );
        }

    }


    @Value
    public static class MockTable {

        public String name;
        public List<String> primaryKeys;
        public List<MockColumnInfo> values;


        public MockTable( String name, List<String> primaryKeys, List<MockColumnInfo> values ) {
            this.name = name;
            this.primaryKeys = primaryKeys;
            this.values = values;
        }


        public LogicalTable toLogical() {
            return new MockLogicalTable( name, primaryKeys, values );
        }

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class MockLogicalTable extends LogicalTable {

        public String name;
        public List<String> primaryKeys;
        public List<MockColumnInfo> mockColumns;


        public MockLogicalTable( String name, List<String> primaryKeys, List<MockColumnInfo> mockColumns ) {
            super( 0, "", 0, EntityType.ENTITY, null, true );
            this.name = name;
            this.primaryKeys = primaryKeys;
            this.mockColumns = mockColumns;
        }


        @Override
        public AlgDataType getTupleType() {
            final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

            long i = 0;

            for ( MockColumnInfo column : mockColumns ) {
                AlgDataType sqlType = column.precision != null ? AlgDataTypeFactory.DEFAULT.createPolyType( column.type, column.precision ) : AlgDataTypeFactory.DEFAULT.createPolyType( column.type );
                fieldInfo.add( i++, column.name, null, sqlType ).nullable( column.nullable );
            }

            return AlgDataTypeImpl.proto( fieldInfo.build() ).apply( AlgDataTypeFactory.DEFAULT );
        }

    }

}
