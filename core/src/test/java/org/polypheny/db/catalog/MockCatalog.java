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

package org.polypheny.db.catalog;

import java.beans.PropertyChangeListener;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalQueryInterface;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;


/**
 * This helper class should serve as a base when implementation-testing different functionalities of
 * Polypheny, which use the catalog.
 * By extending and only implementing the minimal function of the catalog it should
 * provide a clean testing setup
 */
public abstract class MockCatalog extends Catalog {

    @Override
    public void init() {
        throw new NotImplementedException();
    }


    @Override
    public void updateSnapshot() {
        throw new NotImplementedException();
    }


    @Override
    public Map<Long, LogicalUser> getUsers() {
        throw new NotImplementedException();
    }


    @Override
    public Map<Long, LogicalAdapter> getAdapters() {
        throw new NotImplementedException();
    }


    @Override
    public Map<Long, LogicalQueryInterface> getInterfaces() {
        throw new NotImplementedException();
    }


    @Override
    public LogicalRelationalCatalog getLogicalRel( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public LogicalDocumentCatalog getLogicalDoc( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public LogicalGraphCatalog getLogicalGraph( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public AllocationRelationalCatalog getAllocRel( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public AllocationDocumentCatalog getAllocDoc( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public AllocationGraphCatalog getAllocGraph( long namespaceId ) {
        throw new NotImplementedException();
    }



    @Override
    public void addObserver( PropertyChangeListener listener ) {
        super.addObserver( listener );
    }


    @Override
    public void removeObserver( PropertyChangeListener listener ) {
        super.removeObserver( listener );
    }


    @Override
    public Snapshot getSnapshot() {
        throw new NotImplementedException();
    }


    @Override
    public void commit() {
        throw new NotImplementedException();
    }


    @Override
    public long createUser( String name, String password ) {
        throw new NotImplementedException();
    }


    @Override
    public void rollback() {
        throw new NotImplementedException();
    }

    @Override
    public long createNamespace( String name, DataModel dataModel, boolean caseSensitive ) {
        throw new NotImplementedException();
    }


    @Override
    public void renameNamespace( long schemaId, String name ) {
        throw new NotImplementedException();
    }


    @Override
    public void dropNamespace( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public long createAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings, DeployMode mode ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateAdapterSettings( long adapterId, Map<String, String> newSettings ) {
        throw new NotImplementedException();
    }


    @Override
    public void dropAdapter( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public long createQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        throw new NotImplementedException();
    }


    @Override
    public void dropQueryInterface( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public void close() {
        throw new NotImplementedException();
    }


    @Override
    public void clear() {
        throw new NotImplementedException();
    }

}
