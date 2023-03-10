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

import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.PhysicalSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Transaction;


/**
 * This helper class should serve as a base when implementation-testing different functionalities of
 * Polypheny, which use the catalog.
 * By extending and only implementing the minimal function of the catalog it should
 * provide a clean testing setup
 */
public abstract class MockCatalog extends Catalog {

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
    public LogicalEntity getLogicalEntity( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public PhysicalCatalog getPhysical( long namespaceId ) {
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
    public LogicalDocSnapshot getDocSnapshot( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public LogicalGraphSnapshot getGraphSnapshot( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public LogicalRelSnapshot getRelSnapshot( long namespaceId ) {
        throw new NotImplementedException();
    }


    @Override
    public PhysicalSnapshot getPhysicalSnapshot() {
        throw new NotImplementedException();
    }


    @Override
    public AllocSnapshot getAllocSnapshot() {
        throw new NotImplementedException();
    }


    @Override
    public void commit() throws NoTablePrimaryKeyException {
        throw new NotImplementedException();
    }


    @Override
    public long addUser( String name, String password ) {
        throw new NotImplementedException();
    }


    @Override
    public void rollback() {
        throw new NotImplementedException();
    }


    @Override
    public void validateColumns() {
        throw new NotImplementedException();
    }


    @Override
    public void restoreColumnPlacements( Transaction transaction ) {
        throw new NotImplementedException();
    }


    @Override
    public Map<Long, AlgNode> getNodeInfo() {
        throw new NotImplementedException();
    }


    @Override
    public void restoreViews( Transaction transaction ) {
        throw new NotImplementedException();
    }


    private List<CatalogDatabase> getDatabases( Pattern pattern ) {
        throw new NotImplementedException();
    }


    private CatalogDatabase getDatabase( String databaseName ) {
        throw new NotImplementedException();
    }


    private CatalogDatabase getDatabase( long databaseId ) {
        throw new NotImplementedException();
    }



    private List<LogicalNamespace> getSchemas( long databaseId, Pattern schemaNamePattern ) {
        throw new NotImplementedException();
    }


    private LogicalNamespace getNamespace( long databaseId, String schemaName ) throws UnknownSchemaException {
        throw new NotImplementedException();
    }


    @Override
    public long addNamespace( String name, NamespaceType namespaceType, boolean caseSensitive ) {
        throw new NotImplementedException();
    }



    @Override
    public void renameNamespace( long schemaId, String name ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteNamespace( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public long addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        throw new NotImplementedException();
    }


    @Override
    public void updateAdapterSettings( long adapterId, Map<String, String> newSettings ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteAdapter( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public long addQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        throw new NotImplementedException();
    }


    @Override
    public void deleteQueryInterface( long id ) {
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


    @Override
    public void restoreInterfacesIfNecessary() {
        throw new NotImplementedException();
    }

}
