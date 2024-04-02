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
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager.Function4;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
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
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.RunMode;

public abstract class Catalog implements ExtensionPoint {

    public static String DATABASE_NAME = "APP";
    public static String USER_NAME = "pa"; // change with user management

    public static AdapterTemplate defaultStore;
    public static AdapterTemplate defaultSource;
    public static long defaultUserId = 0;
    public static String DEFAULT_NAMESPACE_NAME = "public";
    public static long defaultNamespaceId = 0;
    public static boolean resetDocker;

    protected static List<Runnable> afterInit = new ArrayList<>();

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );
    public boolean isPersistent = false;
    private static Catalog INSTANCE = null;
    public static boolean resetCatalog;
    public static boolean memoryCatalog;
    public static RunMode mode;

    public static final Expression CATALOG_EXPRESSION = Expressions.call( Catalog.class, "getInstance" );

    public static final Expression SNAPSHOT_EXPRESSION = Expressions.call( Catalog.class, "snapshot" );
    public static final Function<Long, Expression> PHYSICAL_EXPRESSION = id -> Expressions.call( CATALOG_EXPRESSION, "getAdapterCatalog", Expressions.constant( id ) );


    public static Catalog setAndGetInstance( Catalog catalog ) {
        if ( INSTANCE != null && Catalog.mode != RunMode.TEST ) {
            throw new GenericRuntimeException( "Setting the Catalog, when already set is not permitted." );
        }
        INSTANCE = catalog;
        return INSTANCE;
    }


    public static Catalog getInstance() {
        if ( INSTANCE == null ) {
            throw new GenericRuntimeException( "Catalog was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public static void afterInit( Runnable action ) {
        afterInit.add( action );
    }


    public abstract void init();

    public abstract void updateSnapshot();

    public abstract void change();

    public abstract void commit();

    public abstract void rollback();

    public abstract LogicalRelationalCatalog getLogicalRel( long namespaceId );

    public abstract LogicalDocumentCatalog getLogicalDoc( long namespaceId );

    public abstract LogicalGraphCatalog getLogicalGraph( long namespaceId );


    public abstract AllocationRelationalCatalog getAllocRel( long namespaceId );

    public abstract AllocationDocumentCatalog getAllocDoc( long namespaceId );

    public abstract AllocationGraphCatalog getAllocGraph( long namespaceId );

    public abstract <S extends AdapterCatalog> Optional<S> getAdapterCatalog( long id );

    public abstract void addStoreSnapshot( AdapterCatalog snapshot );


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
     * Inserts a new user
     *
     * @param name of the user
     * @param password of the user
     * @return the id of the created user
     */
    public abstract long createUser( String name, String password );


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
    public abstract void dropNamespace( long id );


    /**
     * Adds a schema in a specified database
     *
     * @param name The name of the schema
     * @param dataModel The type of this schema
     * @param caseSensitive
     * @return The id of the inserted schema
     */
    public abstract long createNamespace( String name, DataModel dataModel, boolean caseSensitive );

    /**
     * Add an adapter
     *
     * @param uniqueName The unique name of the adapter
     * @param clazz The class name of the adapter
     * @param type The type of adapter
     * @param settings The configuration of the adapter
     * @param mode The deploy mode of the adapter
     * @return The id of the newly added adapter
     */
    public abstract long createAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings, DeployMode mode );

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
    public abstract void dropAdapter( long id );


    /**
     * Add a query interface
     *
     * @param uniqueName The unique name of the query interface
     * @param clazz The class name of the query interface
     * @param settings The configuration of the query interface
     * @return The id of the newly added query interface
     */
    public abstract long createQueryInterface( String uniqueName, String clazz, Map<String, String> settings );

    /**
     * Delete a query interface
     *
     * @param id The id of the query interface to delete
     */
    public abstract void dropQueryInterface( long id );

    public abstract long createAdapterTemplate( Class<? extends Adapter<?>> clazz, String adapterName, String description, List<DeployMode> modes, List<AbstractAdapterSetting> settings, Function4<Long, String, Map<String, String>, Adapter<?>> deployer );


    public abstract void createInterfaceTemplate( String name, QueryInterfaceTemplate queryInterfaceTemplate );

    public abstract void dropInterfaceTemplate( String name );

    @NotNull
    public abstract Map<String, QueryInterfaceTemplate> getInterfaceTemplates();


    public abstract void close();

    public abstract void clear();


    public abstract Snapshot getSnapshot();


    public Snapshot getSnapshot( long id ) {
        return snapshot();
    }


    public static Snapshot snapshot() {
        return INSTANCE.getSnapshot();
    }


    public abstract Map<Long, LogicalUser> getUsers();

    public abstract Map<Long, LogicalAdapter> getAdapters();

    public abstract Map<Long, LogicalQueryInterface> getInterfaces();

    public abstract Map<Long, AdapterTemplate> getAdapterTemplates();


    public abstract void dropAdapterTemplate( long templateId );


    public abstract PropertyChangeListener getChangeListener();


    public abstract void restore( Transaction transaction );


    public abstract void attachCommitConstraint( Supplier<Boolean> constraintChecker, String description );

}
