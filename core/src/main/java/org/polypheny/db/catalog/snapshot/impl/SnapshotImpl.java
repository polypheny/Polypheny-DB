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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.PhysicalSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;

@Value
@Accessors(fluent = true)
public class SnapshotImpl implements Snapshot {


    LogicalRelSnapshot rel;
    LogicalDocSnapshot doc;
    LogicalGraphSnapshot graph;
    AllocSnapshot alloc;
    PhysicalSnapshot physical;
    @Getter
    long id;
    ImmutableMap<Long, CatalogUser> users;

    ImmutableMap<String, CatalogUser> userNames;
    ImmutableMap<Long, CatalogQueryInterface> interfaces;

    ImmutableMap<String, CatalogQueryInterface> interfaceNames;
    ImmutableMap<Long, CatalogAdapter> adapters;

    ImmutableMap<String, CatalogAdapter> adapterNames;

    ImmutableMap<Long, LogicalNamespace> namespaces;

    ImmutableMap<String, LogicalNamespace> namespaceNames;


    public SnapshotImpl( long id, Catalog catalog, Map<Long, LogicalNamespace> namespaces, LogicalRelSnapshot rel, LogicalDocSnapshot doc, LogicalGraphSnapshot graph, AllocSnapshot alloc, PhysicalSnapshot physical ) {
        this.id = id;
        this.rel = rel;
        this.doc = doc;
        this.graph = graph;

        this.namespaces = ImmutableMap.copyOf( namespaces );

        this.namespaceNames = ImmutableMap.copyOf( namespaces.values().stream().collect( Collectors.toMap( n -> n.caseSensitive ? n.name : n.name.toLowerCase(), n -> n ) ) );

        this.alloc = alloc;

        this.physical = physical;
        this.users = ImmutableMap.copyOf( catalog.getUsers() );
        this.userNames = ImmutableMap.copyOf( users.values().stream().collect( Collectors.toMap( u -> u.name, u -> u ) ) );
        this.interfaces = ImmutableMap.copyOf( catalog.getInterfaces() );
        this.interfaceNames = ImmutableMap.copyOf( interfaces.values().stream().collect( Collectors.toMap( i -> i.name, i -> i ) ) );
        this.adapters = ImmutableMap.copyOf( catalog.getAdapters() );
        this.adapterNames = ImmutableMap.copyOf( adapters.values().stream().collect( Collectors.toMap( a -> a.uniqueName, a -> a ) ) );
    }


    @Override
    public @NonNull List<LogicalNamespace> getNamespaces( @Nullable Pattern name ) {
        if ( name == null ) {
            return namespaces.values().asList();
        }
        return namespaces.values().stream().filter( n -> n.caseSensitive ? n.name.matches( name.toRegex() ) : n.name.toLowerCase().matches( name.toLowerCase().toRegex() ) ).collect( Collectors.toList() );
    }


    @Override
    public LogicalNamespace getNamespace( long id ) {
        return namespaces.get( id );
    }


    @Override
    public LogicalNamespace getNamespace( String name ) {
        return namespaceNames.get( name );
    }


    @Override
    public boolean checkIfExistsNamespace( String name ) {
        return namespaceNames.containsKey( name );
    }


    @Override
    public CatalogUser getUser( String name ) {
        return userNames.get( name );
    }


    @Override
    public CatalogUser getUser( long id ) {
        return users.get( id );
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        return adapters.values().asList();
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) {
        return adapterNames.get( uniqueName );
    }


    @Override
    public CatalogAdapter getAdapter( long id ) {
        return adapters.get( id );
    }


    @Override
    public boolean checkIfExistsAdapter( long id ) {
        return adapters.containsKey( id );
    }


    @Override
    public List<CatalogQueryInterface> getQueryInterfaces() {
        return interfaces.values().asList();
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) {
        return interfaceNames.get( uniqueName );
    }


    @Override
    public CatalogQueryInterface getQueryInterface( long id ) {
        return interfaces.get( id );
    }


    @Override
    public List<LogicalTable> getTablesForPeriodicProcessing() {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long id ) {
        CatalogEntity entity = rel.getTable( id );
        if ( entity != null ) {
            return entity;
        }

        entity = doc.getCollection( id );
        if ( entity != null ) {
            return entity;
        }

        return graph.getGraph( id );
    }


    @Override
    public LogicalEntity getLogicalEntity( long id ) {
        LogicalEntity entity = rel.getTable( id );
        if ( entity != null ) {
            return entity;
        }

        entity = doc.getCollection( id );
        if ( entity != null ) {
            return entity;
        }

        return graph.getLogicalGraph( id );
    }


}
