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
import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Transaction;

public abstract class Catalog implements ExtensionPoint {

    public static Adapter defaultStore;
    public static Adapter defaultSource;
    public static int defaultUserId = 0;
    public static long defaultDatabaseId = 0;
    public static boolean resetDocker;
    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );
    public boolean isPersistent = false;
    private static Catalog INSTANCE = null;
    public static boolean resetCatalog;
    public static boolean memoryCatalog;
    public static boolean testMode;

    public static final Expression CATALOG_EXPRESSION = Expressions.call( Catalog.class, "getInstance" );


    public static Catalog setAndGetInstance( Catalog catalog ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Setting the Catalog, when already set is not permitted." );
        }
        INSTANCE = catalog;
        return INSTANCE;
    }


    public static Catalog getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "Catalog was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract void commit() throws NoTablePrimaryKeyException;

    public abstract void rollback();

    public abstract LogicalRelationalCatalog getLogicalRel( long id );

    public abstract LogicalDocumentCatalog getLogicalDoc( long id );

    public abstract LogicalGraphCatalog getLogicalGraph( long id );


    public abstract AllocationRelationalCatalog getAllocRel( long id );

    public abstract AllocationDocumentCatalog getAllocDoc( long id );

    public abstract AllocationGraphCatalog getAllocGraph( long id );

    public abstract PhysicalEntity<?> getPhysicalEntity( long id );

    public abstract Map<Long, AlgNode> getNodeInfo();


    /**
     * Adds a listener which gets notified on updates
     *
     * @param listener which gets added
     */
    public void addObserver( PropertyChangeListener listener ) {
        listeners.addPropertyChangeListener( listener );
    }


    /**
     * Removes a registered observer
     *
     * @param listener which gets removed
     */
    public void removeObserver( PropertyChangeListener listener ) {
        listeners.removePropertyChangeListener( listener );
    }


    /**
     * Restores all interfaces if none are present
     */
    public abstract void restoreInterfacesIfNecessary();

    /**
     * Validates that all columns have a valid placement,
     * else deletes them.
     */
    public abstract void validateColumns();

    /**
     * Restores all columnPlacements in the dedicated store
     */
    public abstract void restoreColumnPlacements( Transaction transaction );

    /**
     * On restart, all AlgNodes used in views and materialized views need to be recreated.
     * Depending on the query language, different methods are used.
     */
    public abstract void restoreViews( Transaction transaction );


    protected final boolean isValidIdentifier( final String str ) {
        return str.length() <= 100 && str.matches( "^[a-z_][a-z0-9_]*$" ) && !str.isEmpty();
    }


    /**
     * Inserts a new user,
     * if a user with the same name already exists, it throws an error // TODO should it?
     *
     * @param name of the user
     * @param password of the user
     * @return the id of the created user
     */
    public abstract int addUser( String name, String password );


    /**
     * Get all schemas which fit to the specified filter pattern.
     * <code>getNamespaces(xid, null, null)</code> returns all schemas of all databases.
     *
     * @param name Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    public abstract @NonNull List<LogicalNamespace> getNamespaces( Pattern name );

    /**
     * Returns the schema with the specified id.
     *
     * @param id The id of the schema
     * @return The schema
     */
    public abstract LogicalNamespace getNamespace( long id );

    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param name The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    public abstract LogicalNamespace getNamespace( String name ) throws UnknownSchemaException;

    /**
     * Adds a schema in a specified database
     *
     * @param name The name of the schema
     * @param namespaceType The type of this schema
     * @param caseSensitive
     * @return The id of the inserted schema
     */
    public abstract long addNamespace( String name, NamespaceType namespaceType, boolean caseSensitive );

    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param name The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     */
    public abstract boolean checkIfExistsNamespace( String name );

    /**
     * Renames a schema
     *
     * @param schemaId The if of the schema to rename
     * @param name New name of the schema
     */
    public abstract void renameNamespace( long schemaId, String name );


    /**
     * Delete a schema from the catalog
     *
     * @param id The id of the schema to delete
     */
    public abstract void deleteNamespace( long id );


    /**
     * Get the user with the specified name
     *
     * @param name The name of the user
     * @return The user
     * @throws UnknownUserException If there is no user with the specified name
     */
    public abstract CatalogUser getUser( String name ) throws UnknownUserException;

    /**
     * Get the user with the specified id.
     *
     * @param id The id of the user
     * @return The user
     */
    public abstract CatalogUser getUser( long id );

    /**
     * Get list of all adapters
     *
     * @return List of adapters
     */
    public abstract List<CatalogAdapter> getAdapters();

    /**
     * Get an adapter by its unique name
     *
     * @return The adapter
     */
    public abstract CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException;

    /**
     * Get an adapter by its id
     *
     * @return The adapter
     */
    public abstract CatalogAdapter getAdapter( long id );

    /**
     * Check if an adapter with the given id exists
     *
     * @param id the id of the adapter
     * @return if the adapter exists
     */
    public abstract boolean checkIfExistsAdapter( long id );

    /**
     * Add an adapter
     *
     * @param uniqueName The unique name of the adapter
     * @param clazz The class name of the adapter
     * @param type The type of adapter
     * @param settings The configuration of the adapter
     * @return The id of the newly added adapter
     */
    public abstract long addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings );

    /**
     * Update settings of an adapter
     *
     * @param adapterId The id of the adapter
     * @param newSettings The new settings for the adapter
     */
    public abstract void updateAdapterSettings( long adapterId, Map<String, String> newSettings );

    /**
     * Delete an adapter
     *
     * @param id The id of the adapter to delete
     */
    public abstract void deleteAdapter( long id );

    /*
     * Get list of all query interfaces
     *
     * @return List of query interfaces
     */
    public abstract List<CatalogQueryInterface> getQueryInterfaces();

    /**
     * Get a query interface by its unique name
     *
     * @param uniqueName The unique name of the query interface
     * @return The CatalogQueryInterface
     */
    public abstract CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException;

    /**
     * Get a query interface by its id
     *
     * @param id The id of the query interface
     * @return The CatalogQueryInterface
     */
    public abstract CatalogQueryInterface getQueryInterface( long id );

    /**
     * Add a query interface
     *
     * @param uniqueName The unique name of the query interface
     * @param clazz The class name of the query interface
     * @param settings The configuration of the query interface
     * @return The id of the newly added query interface
     */
    public abstract long addQueryInterface( String uniqueName, String clazz, Map<String, String> settings );

    /**
     * Delete a query interface
     *
     * @param id The id of the query interface to delete
     */
    public abstract void deleteQueryInterface( long id );


    public abstract void close();

    public abstract void clear();


    public abstract Snapshot getSnapshot( long id );

    //// todo move into snapshot


    public abstract List<AllocationEntity<?>> getAllocationsOnAdapter( long id );


    public abstract List<PhysicalEntity<?>> getPhysicalsOnAdapter( long adapterId );


    public abstract List<CatalogIndex> getIndexes();


    public abstract List<LogicalTable> getTablesForPeriodicProcessing();

}
