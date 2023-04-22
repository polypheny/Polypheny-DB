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
import java.util.Map;
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
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Transaction;

public abstract class Catalog implements ExtensionPoint {

    public static String DATABASE_NAME = "APP";
    public static String USER_NAME = "pa"; // change with user management

    public static Adapter defaultStore;
    public static Adapter defaultSource;
    public static long defaultUserId = 0;
    public static String defaultNamespaceName = "public";
    public static long defaultNamespaceId = 0;
    public static boolean resetDocker;
    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );
    public boolean isPersistent = false;
    private static Catalog INSTANCE = null;
    public static boolean resetCatalog;
    public static boolean memoryCatalog;
    public static boolean testMode;

    public static final Expression CATALOG_EXPRESSION = Expressions.call( Catalog.class, "getInstance" );

    public static final Expression SNAPSHOT_EXPRESSION = Expressions.call( Catalog.class, "snapshot" );
    public static final Expression PHYSICAL_EXPRESSION = Expressions.call( SNAPSHOT_EXPRESSION, "physical" );


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


    public abstract void init();

    public abstract void updateSnapshot();

    public abstract void commit();

    public abstract void rollback();

    public abstract LogicalRelationalCatalog getLogicalRel( long namespaceId );

    public abstract LogicalDocumentCatalog getLogicalDoc( long namespaceId );

    public abstract LogicalGraphCatalog getLogicalGraph( long namespaceId );


    public abstract AllocationRelationalCatalog getAllocRel( long namespaceId );

    public abstract AllocationDocumentCatalog getAllocDoc( long namespaceId );

    public abstract AllocationGraphCatalog getAllocGraph( long namespaceId );


    public abstract Map<Long, AlgNode> getNodeInfo();

    public abstract <S extends StoreCatalog> S getStoreSnapshot( long id );

    public abstract void addStoreSnapshot( StoreCatalog snapshot );


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
     * Inserts a new user
     *
     * @param name of the user
     * @param password of the user
     * @return the id of the created user
     */
    public abstract long addUser( String name, String password );


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
     * Adds a schema in a specified database
     *
     * @param name The name of the schema
     * @param namespaceType The type of this schema
     * @param caseSensitive
     * @return The id of the inserted schema
     */
    public abstract long addNamespace( String name, NamespaceType namespaceType, boolean caseSensitive );

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


    public abstract Snapshot getSnapshot();


    public Snapshot getSnapshot( long id ) {
        return snapshot();
    }


    public static Snapshot snapshot() {
        return INSTANCE.getSnapshot();
    }


    public abstract Map<Long, CatalogUser> getUsers();

    public abstract Map<Long, CatalogAdapter> getAdapters();

    public abstract Map<Long, CatalogQueryInterface> getInterfaces();

}
