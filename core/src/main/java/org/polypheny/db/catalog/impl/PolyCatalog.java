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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.Function5;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.algebra.AlgRoot;
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
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.impl.allocation.PolyAllocDocCatalog;
import org.polypheny.db.catalog.impl.allocation.PolyAllocGraphCatalog;
import org.polypheny.db.catalog.impl.allocation.PolyAllocRelCatalog;
import org.polypheny.db.catalog.impl.logical.DocumentCatalog;
import org.polypheny.db.catalog.impl.logical.GraphCatalog;
import org.polypheny.db.catalog.impl.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.persistance.FilePersister;
import org.polypheny.db.catalog.persistance.InMemoryPersister;
import org.polypheny.db.catalog.persistance.Persister;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.snapshot.impl.SnapshotBuilder;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.Pair;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 */
@Slf4j
public class PolyCatalog extends Catalog implements PolySerializable {

    private final static JsonMapper MAPPER = JsonMapper.builder()
            .configure( MapperFeature.AUTO_DETECT_CREATORS, false )
            .configure( MapperFeature.AUTO_DETECT_FIELDS, false )
            .configure( MapperFeature.AUTO_DETECT_GETTERS, false )
            .configure( MapperFeature.AUTO_DETECT_IS_GETTERS, false )
            .configure( MapperFeature.AUTO_DETECT_SETTERS, false )
            .configure( SerializationFeature.FAIL_ON_EMPTY_BEANS, false )
            .build();

    @Getter
    private final BinarySerializer<PolyCatalog> serializer = PolySerializable.buildSerializer( PolyCatalog.class );

    // indicates if the state has advanced and the snapshot has to be recreated or can be reused // trx without ddl
    private long lastCommitSnapshotId = 0;

    @Serialize
    @JsonProperty
    public final Map<Long, LogicalCatalog> logicalCatalogs;

    @Serialize
    @JsonProperty
    public final Map<Long, AllocationCatalog> allocationCatalogs;

    @Serialize
    @JsonProperty
    @Getter
    public final Map<Long, LogicalUser> users;

    @Serialize
    @JsonProperty
    @Getter
    public final Map<Long, LogicalAdapter> adapters;

    @Serialize
    @JsonProperty
    @Getter
    public final Map<Long, LogicalQueryInterface> interfaces;

    @Getter
    public final Map<Long, AdapterTemplate> adapterTemplates;

    @Getter
    public final Map<String, QueryInterfaceTemplate> interfaceTemplates;

    @Getter
    public final Map<Long, AdapterCatalog> adapterCatalogs;

    @Serialize
    @JsonProperty
    public final Map<Long, AdapterRestore> adapterRestore;

    @Serialize
    @JsonProperty
    public final IdBuilder idBuilder;

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
                Map.of(),
                IdBuilder.getInstance() );
    }


    public PolyCatalog(
            @Deserialize("users") Map<Long, LogicalUser> users,
            @Deserialize("logicalCatalogs") Map<Long, LogicalCatalog> logicalCatalogs,
            @Deserialize("allocationCatalogs") Map<Long, AllocationCatalog> allocationCatalogs,
            @Deserialize("adapterRestore") Map<Long, AdapterRestore> adapterRestore,
            @Deserialize("adapters") Map<Long, LogicalAdapter> adapters,
            @Deserialize("interfaces") Map<Long, LogicalQueryInterface> interfaces,
            @Deserialize("idBuilder") IdBuilder idBuilder ) {
        // persistent data
        this.idBuilder = idBuilder;
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


    @Override
    public String getJson() {
        try {
            return MAPPER.writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public synchronized void commit() {
        if ( !this.dirty.get() ) {
            log.debug( "Nothing changed" );
            return;
        }

        log.debug( "commit" );

        this.adapterRestore.clear();
        adapterCatalogs.forEach( ( id, catalog ) -> {
            Map<Long, List<PhysicalEntity>> restore = catalog.allocToPhysicals.entrySet().stream().collect( Collectors.toMap( Entry::getKey, a -> a.getValue().stream().map( key -> catalog.physicals.get( key ).normalize() ).toList() ) );
            this.adapterRestore.put( id, new AdapterRestore( id, restore, catalog.allocations ) );
        } );

        this.backup = serialize();

        updateSnapshot();
        persister.write( backup );
        this.dirty.set( false );

        this.lastCommitSnapshotId = snapshot.id();
    }


    public void rollback() {

        restoreLastState();

        log.debug( "rollback" );

        if ( lastCommitSnapshotId != snapshot.id() ) {
            updateSnapshot();

            lastCommitSnapshotId = snapshot.id();
        }
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
        idBuilder.restore( old.idBuilder );
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
    public Optional<AdapterCatalog> getAdapterCatalog( long id ) {
        return Optional.ofNullable( adapterCatalogs.get( id ) );
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


    public long createNamespace( String name, DataModel dataModel, boolean caseSensitive, boolean hidden ) {
        // cannot separate namespace and entity ids, as there are models which have their entity on the namespace level
        long id = idBuilder.getNewLogicalId();
        LogicalNamespace namespace = new LogicalNamespace( id, name, dataModel, caseSensitive, hidden );

        Pair<LogicalCatalog, AllocationCatalog> catalogs = switch ( dataModel ) {
            case RELATIONAL -> Pair.of( new RelationalCatalog( namespace ), new PolyAllocRelCatalog( namespace ) );
            case DOCUMENT -> Pair.of( new DocumentCatalog( namespace ), new PolyAllocDocCatalog( namespace ) );
            case GRAPH -> Pair.of( new GraphCatalog( namespace ), new PolyAllocGraphCatalog( namespace ) );
        };

        logicalCatalogs.put( id, catalogs.left );
        allocationCatalogs.put( id, catalogs.right );

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
        adapterCatalogs.remove( id );
        adapterRestore.remove( id );
        adapters.remove( id );
        change();
    }


    @Override
    public long createQueryInterface( String uniqueName, String interfaceName, Map<String, String> settings ) {
        long id = idBuilder.getNewInterfaceId();

        synchronized ( this ) {
            if ( interfaces.values().stream().anyMatch( l -> l.name.equals( uniqueName ) ) ) {
                throw new GenericRuntimeException( "There is already a query interface with name " + uniqueName );
            }
            interfaces.put( id, new LogicalQueryInterface( id, uniqueName, interfaceName, settings ) );
        }

        change();
        return id;
    }


    @Override
    public void dropQueryInterface( long id ) {
        interfaces.remove( id );
        change();
    }


    @Override
    public long createAdapterTemplate( Class<? extends Adapter<?>> clazz, String adapterName, String description, List<DeployMode> modes, List<AbstractAdapterSetting> settings, Function5<Long, String, Map<String, String>, DeployMode, Adapter<?>> deployer ) {
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

        restoreViews( transaction );

        updateSnapshot();
    }


    private void restoreViews( Transaction transaction ) {
        Statement statement = transaction.createStatement();
        snapshot.rel().getTables( (Pattern) null, null ).forEach( table -> {
            if ( table instanceof LogicalView view ) {
                Processor sqlProcessor = statement.getTransaction().getProcessor( view.language );
                Node node = sqlProcessor.parse( view.query ).get( 0 );
                AlgRoot algRoot = sqlProcessor.translate( statement,
                        ParsedQueryContext.builder()
                                .query( view.query )
                                .language( view.language )
                                .queryNode( sqlProcessor.validate(
                                        statement.getTransaction(), node, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ).left )
                                .origin( statement.getTransaction().getOrigin() )
                                .build() );
                getLogicalRel( view.namespaceId ).setNodeAndCollation( view.id, algRoot.alg, algRoot.collation );
            }
        } );
        transaction.commit();
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
