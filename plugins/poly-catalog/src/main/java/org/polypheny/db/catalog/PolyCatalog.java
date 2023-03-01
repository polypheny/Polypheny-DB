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

package org.polypheny.db.catalog;

import com.google.common.collect.ImmutableMap;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logical.document.DocumentCatalog;
import org.polypheny.db.catalog.logical.graph.GraphCatalog;
import org.polypheny.db.catalog.logical.relational.RelationalCatalog;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.FullSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Transaction;


/**
 * Central catalog, which distributes the operations to the corresponding model catalogs.
 * Object are as follows:
 * Namespace -> Schema (Relational), Graph (Graph), Database (Document)
 * Entity -> Table (Relational), does not exist (Graph), Collection (Document)
 * Field -> Column (Relational), does not exist (Graph), Field (Document)
 */
@Slf4j
public class PolyCatalog extends Catalog implements Serializable {

    @Getter
    public final BinarySerializer<PolyCatalog> serializer = Serializable.builder.get().build( PolyCatalog.class );

    @Serialize
    public final Map<Long, LogicalCatalog> catalogs;

    @Serialize
    public final Map<Long, CatalogUser> users;

    @Serialize
    public final Map<Long, CatalogAdapter> adapters;

    @Serialize
    public final Map<Long, CatalogQueryInterface> interfaces;

    private final IdBuilder idBuilder = new IdBuilder();
    private FullSnapshot fullSnapshot;

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public PolyCatalog() {
        this( new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>() );
    }


    public PolyCatalog(
            @Deserialize("users") Map<Long, CatalogUser> users,
            @Deserialize("catalogs") Map<Long, LogicalCatalog> catalogs,
            @Deserialize("adapters") Map<Long, CatalogAdapter> adapters,
            @Deserialize("interfaces") Map<Long, CatalogQueryInterface> interfaces ) {

        this.users = users;
        this.catalogs = catalogs;
        this.adapters = adapters;
        this.interfaces = interfaces;
        updateSnapshot();
    }


    private void updateSnapshot() {
        this.fullSnapshot = new FullSnapshot( idBuilder.getNewSnapshotId(), catalogs );
    }


    private void change() {
        // empty for now
    }


    public void commit() {
        log.debug( "commit" );
        updateSnapshot();
    }


    public void rollback() {
        log.debug( "rollback" );
    }


    @Override
    @Deprecated
    public Map<Long, AlgNode> getNodeInfo() {
        return null;
    }


    @Override
    @Deprecated
    public void restoreInterfacesIfNecessary() {

    }


    @Override
    @Deprecated
    public void validateColumns() {

    }


    @Override
    @Deprecated
    public void restoreColumnPlacements( Transaction transaction ) {

    }


    @Override
    @Deprecated
    public void restoreViews( Transaction transaction ) {

    }


    @Override
    public int addUser( String name, String password ) {
        return 0;
    }


    public long addNamespace( String name, NamespaceType namespaceType, boolean caseSensitive ) {
        long id = idBuilder.getNewNamespaceId();
        LogicalNamespace namespace = new LogicalNamespace( id, name, namespaceType, caseSensitive );

        switch ( namespaceType ) {
            case RELATIONAL:
                catalogs.put( id, new RelationalCatalog( namespace, idBuilder ) );
                break;
            case DOCUMENT:
                catalogs.put( id, new DocumentCatalog( namespace, idBuilder ) );
                break;
            case GRAPH:
                catalogs.put( id, new GraphCatalog( namespace, idBuilder ) );
                break;
        }
        change();
        return id;
    }


    @Override
    public @NonNull List<LogicalNamespace> getNamespaces( Pattern name ) {
        return catalogs.values().stream().filter( c ->
                        c.getLogicalNamespace().caseSensitive
                                ? c.getLogicalNamespace().name.toLowerCase( Locale.ROOT ).matches( name.pattern )
                                : c.getLogicalNamespace().name.matches( name.pattern ) )
                .map( LogicalCatalog::getLogicalNamespace ).collect( Collectors.toList() );
    }


    @Override
    public LogicalNamespace getNamespace( long id ) {
        return catalogs.get( id ).getLogicalNamespace();
    }


    @Override
    public LogicalNamespace getNamespace( String name ) throws UnknownSchemaException {
        List<LogicalNamespace> namespaces = getNamespaces( Pattern.of( name ) );
        if ( namespaces.isEmpty() ) {
            return null;
        } else if ( namespaces.size() > 1 ) {
            throw new RuntimeException( "multiple namespaces retrieved" );
        }
        return namespaces.get( 0 );

    }


    @Override
    public boolean checkIfExistsNamespace( String name ) {
        return !getNamespaces( Pattern.of( name ) ).isEmpty();
    }


    @Override
    public void renameNamespace( long id, String name ) {
        if ( catalogs.get( id ) == null ) {
            return;
        }
        catalogs.get( id ).withLogicalNamespace( catalogs.get( id ).getLogicalNamespace().withName( name ) );

        change();
    }


    @Override
    public void deleteNamespace( long id ) {
        catalogs.remove( id );

        change();
    }


    @Override
    public CatalogUser getUser( String name ) throws UnknownUserException {
        return users.values().stream().filter( u -> u.name.equals( name ) ).findFirst().orElse( null );
    }


    @Override
    public CatalogUser getUser( long id ) {
        return users.get( id );
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        return List.copyOf( adapters.values() );
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        return adapters.values().stream().filter( a -> a.uniqueName.equals( uniqueName ) ).findFirst().orElse( null );
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
    public long addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        long id = idBuilder.getNewAdapterId();
        adapters.put( id, new CatalogAdapter( id, uniqueName, clazz, type, settings ) );
        return id;
    }


    @Override
    public void updateAdapterSettings( long adapterId, Map<String, String> newSettings ) {
        if ( !adapters.containsKey( adapterId ) ) {
            return;
        }
        adapters.put( adapterId, adapters.get( adapterId ).withSettings( ImmutableMap.copyOf( newSettings ) ) );
    }


    @Override
    public void deleteAdapter( long id ) {
        adapters.remove( id );
    }


    @Override
    public List<CatalogQueryInterface> getQueryInterfaces() {
        return List.copyOf( interfaces.values() );
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        return interfaces.values().stream().filter( i -> i.name.equals( uniqueName ) ).findFirst().orElse( null );
    }


    @Override
    public CatalogQueryInterface getQueryInterface( long id ) {
        return interfaces.get( id );
    }


    @Override
    public long addQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        long id = idBuilder.getNewInterfaceId();

        interfaces.put( id, new CatalogQueryInterface( id, uniqueName, clazz, settings ) );

        return id;
    }


    @Override
    public void deleteQueryInterface( long id ) {
        interfaces.remove( id );
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
    public Snapshot getSnapshot( long id ) {
        return null;
    }


    @Override
    public PolyCatalog copy() {
        return deserialize( serialize(), PolyCatalog.class );
    }

}
