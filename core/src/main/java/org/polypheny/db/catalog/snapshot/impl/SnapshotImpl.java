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
import lombok.NonNull;
import lombok.Value;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.PhysicalSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;

@Value
public class SnapshotImpl implements Snapshot {

    ImmutableMap<Long, LogicalRelSnapshot> relationals;
    ImmutableMap<Long, LogicalDocSnapshot> documents;
    ImmutableMap<Long, LogicalGraphSnapshot> graphs;
    AllocSnapshot alloc;
    PhysicalSnapshot physical;
    long id;


    public SnapshotImpl( long id, Map<Long, LogicalRelSnapshot> relationals, Map<Long, LogicalDocSnapshot> documents, Map<Long, LogicalGraphSnapshot> graphs, AllocSnapshot alloc, PhysicalSnapshot physical ) {
        this.id = id;
        this.relationals = ImmutableMap.copyOf( relationals );
        this.documents = ImmutableMap.copyOf( documents );
        this.graphs = ImmutableMap.copyOf( graphs );

        this.alloc = alloc;

        this.physical = physical;
    }


    @Override
    public long getId() {
        return 0;
    }


    @Override
    public @NonNull List<LogicalNamespace> getNamespaces( Pattern name ) {
        return null;
    }


    @Override
    public LogicalNamespace getNamespace( long id ) {
        return null;
    }


    @Override
    public LogicalNamespace getNamespace( String name ) {
        return null;
    }


    @Override
    public boolean checkIfExistsNamespace( String name ) {
        return false;
    }


    @Override
    public CatalogUser getUser( String name ) throws UnknownUserException {
        return null;
    }


    @Override
    public CatalogUser getUser( long id ) {
        return null;
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        return null;
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        return null;
    }


    @Override
    public CatalogAdapter getAdapter( long id ) {
        return null;
    }


    @Override
    public boolean checkIfExistsAdapter( long id ) {
        return false;
    }


    @Override
    public List<CatalogQueryInterface> getQueryInterfaces() {
        return null;
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        return null;
    }


    @Override
    public CatalogQueryInterface getQueryInterface( long id ) {
        return null;
    }


    @Override
    public List<AllocationEntity<?>> getAllocationsOnAdapter( long id ) {
        return null;
    }


    @Override
    public List<PhysicalEntity<?>> getPhysicalsOnAdapter( long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogIndex> getIndexes() {
        return null;
    }


    @Override
    public List<LogicalTable> getTablesForPeriodicProcessing() {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long id ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long namespaceId, String name ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long namespaceId, Pattern name ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( List<String> names ) {
        return null;
    }


    @Override
    public boolean checkIfExistsEntity( String entityName ) {
        return false;
    }


    @Override
    public boolean checkIfExistsEntity( long tableId ) {
        return false;
    }


    @Override
    public LogicalNamespace getLogicalNamespace() {
        return null;
    }


    @Override
    public LogicalEntity getEntity( String name ) {
        return null;
    }


    @Override
    public LogicalDocSnapshot getDocSnapshot( long namespaceId ) {
        return documents.get( namespaceId );
    }


    @Override
    public LogicalGraphSnapshot getGraphSnapshot( long namespaceId ) {
        return graphs.get( namespaceId );
    }


    @Override
    public LogicalRelSnapshot getRelSnapshot( long namespaceId ) {
        return relationals.get( namespaceId );
    }


    @Override
    public PhysicalSnapshot getPhysicalSnapshot() {
        return physical;
    }


    @Override
    public AllocSnapshot getAllocSnapshot() {
        return alloc;
    }

}
