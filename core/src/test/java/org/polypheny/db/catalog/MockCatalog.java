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

import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.transaction.Transaction;


/**
 * This helper class should serve as a base when implementation-testing different functionalities of
 * Polypheny, which use the catalog.
 * By extending and only implementing the minimal function of the catalog it should
 * provide a clean testing setup
 */
public abstract class MockCatalog extends Catalog {


    @Override
    public void commit() throws NoTablePrimaryKeyException {
        throw new NotImplementedException();
    }


    @Override
    public int addUser( String name, String password ) {
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


    @Override
    public @NonNull List<LogicalNamespace> getNamespaces( Pattern name ) {
        throw new NotImplementedException();
    }


    private List<LogicalNamespace> getSchemas( long databaseId, Pattern schemaNamePattern ) {
        throw new NotImplementedException();
    }


    @Override
    public LogicalNamespace getNamespace( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public LogicalNamespace getNamespace( String name ) throws UnknownSchemaException {
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
    public boolean checkIfExistsNamespace( String name ) {
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
    public CatalogUser getUser( String name ) throws UnknownUserException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogUser getUser( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public List<CatalogAdapter> getAdapters() {
        throw new NotImplementedException();
    }


    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogAdapter getAdapter( long id ) {
        throw new NotImplementedException();
    }


    @Override
    public boolean checkIfExistsAdapter( long id ) {
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
    public List<CatalogQueryInterface> getQueryInterfaces() {
        throw new NotImplementedException();
    }


    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        throw new NotImplementedException();
    }


    @Override
    public CatalogQueryInterface getQueryInterface( long id ) {
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
    public List<PhysicalEntity<?>> getPhysicalsOnAdapter( long tableId ) {
        throw new NotImplementedException();
    }


    @Override
    public void restoreInterfacesIfNecessary() {
        throw new NotImplementedException();
    }

}
