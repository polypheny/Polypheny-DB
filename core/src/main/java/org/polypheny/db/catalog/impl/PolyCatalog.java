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

package org.polypheny.db.catalog.impl;

import com.google.common.collect.ImmutableMap;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.Function4;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalQueryInterface;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.impl.allocation.PolyAllocDocCatalog;
import org.polypheny.db.catalog.impl.allocation.PolyAllocGraphCatalog;
import org.polypheny.db.catalog.impl.allocation.PolyAllocRelCatalog;
import org.polypheny.db.catalog.impl.logical.DocumentCatalog;
import org.polypheny.db.catalog.impl.logical.GraphCatalog;
import org.polypheny.db.catalog.impl.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.persistance.FilePersister;
import org.polypheny.db.catalog.persistance.InMemoryPersister;
import org.polypheny.db.catalog.persistance.Persister;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.snapshot.impl.SnapshotBuilder;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.Pair;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 */
@Slf4j
public class PolyCatalog extends Catalog implements PolySerializable {

    @Getter
    private final BinarySerializer<PolyCatalog> serializer = PolySerializable.buildSerializer( PolyCatalog.class );

    /**
     * Constraints which have to be met before a commit can be executed.
     */
    private final Collection<Pair<Supplier<Boolean>, String>> commitConstraints = new ConcurrentLinkedDeque<>();


    @Serialize
    public final Map<Long, LogicalCatalog> logicalCatalogs;

    @Serialize
    public final Map<Long, AllocationCatalog> allocationCatalogs;

    @Serialize
    @Getter
    public final Map<Long, LogicalUser> users;

    @Serialize
    @Getter
    public final Map<Long, LogicalAdapter> adapters;

    @Serialize
    @Getter
    public final Map<Long, LogicalQueryInterface> interfaces;

    @Getter
    public final Map<Long, AdapterTemplate> adapterTemplates;

    @Getter
    public final Map<String, QueryInterfaceTemplate> interfaceTemplates;

    @Getter
    public final Map<Long, AdapterCatalog> adapterCatalogs;

    @Serialize
    public final Map<Long, AdapterRestore> adapterRestore;

    public final IdBuilder idBuilder = IdBuilder.getInstance();
    private final Persister persister;

    @Getter
    private Snapshot snapshot;
    private String backup;

    private final AtomicBoolean dirty = new AtomicBoolean( false );

    @Getter
    PropertyChangeListener changeListener = evt -> dirty.set( true );


    public PolyCatalog() {
        this(
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of() );

    }


    public PolyCatalog(
            @Deserialize("users") Map<Long, LogicalUser> users,
            @Deserialize("logicalCatalogs") Map<Long, LogicalCatalog> logicalCatalogs,
            @Deserialize("allocationCatalogs") Map<Long, AllocationCatalog> allocationCatalogs,
            @Deserialize("adapterRestore") Map<Long, AdapterRestore> adapterRestore,
            @Deserialize("adapters") Map<Long, LogicalAdapter> adapters,
            @Deserialize("interfaces") Map<Long, LogicalQueryInterface> interfaces ) {
        // persistent data
        this.users = new ConcurrentHashMap<>( users );
        this.logicalCatalogs = new ConcurrentHashMap<>( logicalCatalogs );
        this.allocationCatalogs = new ConcurrentHashMap<>( allocationCatalogs );
        this.adapters = new ConcurrentHashMap<>( adapters );
        this.interfaces = new ConcurrentHashMap<>( interfaces );
        this.adapterRestore = new ConcurrentHashMap<>( adapterRestore );

        // temporary data
        this.adapterTemplates = new ConcurrentHashMap<>();
        this.adapterCatalogs = new ConcurrentHashMap<>();
        this.interfaceTemplates = new ConcurrentHashMap<>();

        this.persister = memoryCatalog ? new InMemoryPersister() : new FilePersister();

    }


    @Override
    public void init() {
        updateSnapshot();

        Catalog.afterInit.forEach( Runnable::run );
    }


    @Override
    public void updateSnapshot() {
        this.snapshot = SnapshotBuilder.createSnapshot( idBuilder.getNewSnapshotId(), this, logicalCatalogs, allocationCatalogs );

        this.listeners.firePropertyChange( "snapshot", null, this.snapshot );
    }


    @Override
    public void change() {
        // empty for now
        this.dirty.set( true );
        updateSnapshot();
    }


    public synchronized void commit() {
        if ( !this.dirty.get() ) {
            log.debug( "Nothing changed" );
            return;
        }
        // check constraints e.g. primary key constraints
        List<Pair<Boolean, String>> fails = commitConstraints
                .stream()
                .map( c -> Pair.of( c.left.get(), c.right ) )
                .filter( c -> !c.left )
                .toList();

        if ( !fails.isEmpty() ) {
            commitConstraints.clear();
            throw new GenericRuntimeException( "DDL constraints not met: \n" + fails.stream().map( f -> f.right ).collect( Collectors.joining( ",\n " ) ) + "." );
        }

        log.debug( "commit" );

        this.adapterRestore.clear();
        adapterCatalogs.forEach( ( id, catalog ) -> {
            Map<Long, List<PhysicalEntity>> restore = catalog.allocToPhysicals.entrySet().stream().map( a -> Pair.of( a, a.getValue().stream().map( key -> catalog.physicals.get( key ).normalize() ).toList() ) ).collect( Collectors.toMap( a -> a.getKey().getKey(), Pair::getValue ) );
            this.adapterRestore.put( id, new AdapterRestore( id, restore, catalog.allocations ) );
        } );

        this.backup = serialize();

        updateSnapshot();
        persister.write( backup );
        this.dirty.set( false );
        this.commitConstraints.clear();
    }


    public void rollback() {
        restoreLastState();

        log.debug( "rollback" );

        updateSnapshot();

    }


    private void restoreLastState() {
        PolyCatalog old = PolySerializable.deserialize( backup, getSerializer() );

        users.clear();
        users.putAll( old.users );
        logicalCatalogs.clear();
        logicalCatalogs.putAll( old.logicalCatalogs );
        allocationCatalogs.clear();
        allocationCatalogs.putAll( old.allocationCatalogs );
        adapters.clear();
        adapters.putAll( old.adapters );
        interfaces.clear();
        interfaces.putAll( old.interfaces );
        adapterRestore.clear();
        adapterRestore.putAll( old.adapterRestore );
    }


    private void validateNamespaceType( long id, DataModel type ) {
        if ( logicalCatalogs.get( id ).getLogicalNamespace().dataModel != type ) {
            throw new GenericRuntimeException( "Error while retrieving namespace type" );
        }
    }


    @Override
    public LogicalRelationalCatalog getLogicalRel( long namespaceId ) {
        validateNamespaceType( namespaceId, DataModel.RELATIONAL );
        return (LogicalRelationalCatalog) logicalCatalogs.get( namespaceId );
    }


    @Override
    public LogicalDocumentCatalog getLogicalDoc( long namespaceId ) {
        validateNamespaceType( namespaceId, DataModel.DOCUMENT );
        return (LogicalDocumentCatalog) logicalCatalogs.get( namespaceId );
    }


    @Override
    public LogicalGraphCatalog getLogicalGraph( long namespaceId ) {
        validateNamespaceType( namespaceId, DataModel.GRAPH );
        return (LogicalGraphCatalog) logicalCatalogs.get( namespaceId );
    }


    @Override
    public AllocationRelationalCatalog getAllocRel( long namespaceId ) {
        validateNamespaceType( namespaceId, DataModel.RELATIONAL );
        return (AllocationRelationalCatalog) allocationCatalogs.get( namespaceId );
    }


    @Override
    public AllocationDocumentCatalog getAllocDoc( long namespaceId ) {
        validateNamespaceType( namespaceId, DataModel.DOCUMENT );
        return (AllocationDocumentCatalog) allocationCatalogs.get( namespaceId );
    }


    @Override
    public AllocationGraphCatalog getAllocGraph( long namespaceId ) {
        validateNamespaceType( namespaceId, DataModel.GRAPH );
        return (AllocationGraphCatalog) allocationCatalogs.get( namespaceId );
    }


    @Override
    public <S extends AdapterCatalog> Optional<S> getAdapterCatalog( long id ) {
        return Optional.ofNullable( (S) adapterCatalogs.get( id ) );
    }


    @Override
    public void addStoreSnapshot( AdapterCatalog snapshot ) {
        adapterCatalogs.put( snapshot.adapterId, snapshot );
    }


    @Override
    public long createUser( String name, String password ) {
        long id = idBuilder.getNewUserId();
        users.put( id, new LogicalUser( id, name, password ) );
        return id;
    }


    public long createNamespace( String name, DataModel dataModel, boolean caseSensitive ) {
        // cannot separate namespace and entity ids, as there are models which have their entity on the namespace level
        long id = idBuilder.getNewLogicalId();
        LogicalNamespace namespace = new LogicalNamespace( id, name, dataModel, caseSensitive );

        switch ( dataModel ) {
            case RELATIONAL:
                logicalCatalogs.put( id, new RelationalCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocRelCatalog( namespace ) );
                break;
            case DOCUMENT:
                logicalCatalogs.put( id, new DocumentCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocDocCatalog( namespace ) );
                break;
            case GRAPH:
                logicalCatalogs.put( id, new GraphCatalog( namespace ) );
                allocationCatalogs.put( id, new PolyAllocGraphCatalog( namespace ) );
                break;
        }
        change();
        return id;
    }


    @Override
    public void renameNamespace( long id, String name ) {
        if ( logicalCatalogs.get( id ) == null ) {
            return;
        }

        logicalCatalogs.put( id, logicalCatalogs.get( id ).withLogicalNamespace( logicalCatalogs.get( id ).getLogicalNamespace().withName( name ) ) );

        change();
    }


    @Override
    public void dropNamespace( long id ) {
        logicalCatalogs.remove( id );

        change();
    }


    @Override
    public long createAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings, DeployMode mode ) {
        long id = idBuilder.getNewAdapterId();
        adapters.put( id, new LogicalAdapter( id, uniqueName, clazz, type, mode, settings ) );
        change();
        return id;
    }


    @Override
    public void updateAdapterSettings( long adapterId, Map<String, String> newSettings ) {
        if ( !adapters.containsKey( adapterId ) ) {
            return;
        }
        adapters.put( adapterId, adapters.get( adapterId ).toBuilder().settings( ImmutableMap.copyOf( newSettings ) ).build() );
        change();
    }


    @Override
    public void dropAdapter( long id ) {
        adapters.remove( id );
        change();
    }


    @Override
    public long createQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        long id = idBuilder.getNewInterfaceId();

        interfaces.put( id, new LogicalQueryInterface( id, uniqueName, clazz, settings ) );

        change();
        return id;
    }


    @Override
    public void dropQueryInterface( long id ) {
        interfaces.remove( id );
        change();
    }


    @Override
    public long createAdapterTemplate( Class<? extends Adapter<?>> clazz, String adapterName, String description, List<DeployMode> modes, List<AbstractAdapterSetting> settings, Function4<Long, String, Map<String, String>, Adapter<?>> deployer ) {
        long id = idBuilder.getNewAdapterTemplateId();
        adapterTemplates.put( id, new AdapterTemplate( id, clazz, adapterName, settings, modes, description, deployer ) );
        change();
        return id;
    }


    @Override
    public void createInterfaceTemplate( String name, QueryInterfaceTemplate queryInterfaceTemplate ) {
        interfaceTemplates.put( name, queryInterfaceTemplate );
        change();
    }


    @Override
    public void dropInterfaceTemplate( String name ) {
        interfaceTemplates.remove( name );
        change();
    }


    @Override
    public void dropAdapterTemplate( long templateId ) {
        adapterTemplates.remove( templateId );
        change();
    }


    @Override
    public void restore( Transaction transaction ) {
        this.backup = persister.read();
        if ( this.backup == null || this.backup.isEmpty() ) {
            log.warn( "No file found to restore" );
            return;
        }
        // set old state;
        restoreLastState();
        // only for templates
        this.snapshot = SnapshotBuilder.createSnapshot( idBuilder.getNewSnapshotId(), this, Map.of(), Map.of() );
        AdapterManager.getInstance().restoreAdapters( List.copyOf( adapters.values() ) );

        adapterRestore.forEach( ( id, restore ) -> {
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( id ).orElseThrow();
            restore.activate( adapter, transaction.createStatement().getPrepareContext() );
        } );

        updateSnapshot();
    }


    @Override
    public void attachCommitConstraint( Supplier<Boolean> constraintChecker, String description ) {
        commitConstraints.add( Pair.of( constraintChecker, description ) );
    }


    @Override
    public void close() {
        log.error( "closing" );
    }


    @Override
    public void clear() {
        log.error( "clearing" );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyCatalog.class );
    }

}
