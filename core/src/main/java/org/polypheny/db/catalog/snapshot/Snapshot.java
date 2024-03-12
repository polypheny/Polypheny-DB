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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalQueryInterface;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;

public interface Snapshot extends OperatorTable {

    long id();


    /**
     * Get all namespaces which fit to the specified filter pattern.
     *
     * @param name Pattern for the namespace name. null returns all.
     * @return List of namespaces which fit to the specified filter. If there is no namespace which meets the criteria, an empty list is returned.
     */
    @NotNull List<LogicalNamespace> getNamespaces( @Nullable Pattern name );

    /**
     * Returns the namespace with the specified id.
     *
     * @param id The id of the namespace
     * @return The namespace
     */
    @NotNull Optional<LogicalNamespace> getNamespace( long id );

    /**
     * Returns the namespace with the given name.
     *
     * @param name The name of the namespace
     * @return The namespace
     */
    @NotNull Optional<LogicalNamespace> getNamespace( String name );


    /**
     * Get the user with the specified name
     *
     * @param name The name of the user
     * @return The user
     */
    @NotNull Optional<LogicalUser> getUser( String name );

    /**
     * Get the user with the specified id.
     *
     * @param id The id of the user
     * @return The user
     */
    @NotNull Optional<LogicalUser> getUser( long id );

    /**
     * Get list of all adapters
     *
     * @return List of adapters
     */
    List<LogicalAdapter> getAdapters();

    /**
     * Get an adapter by its unique name
     *
     * @return The adapter
     */
    @NotNull Optional<LogicalAdapter> getAdapter( String uniqueName );

    /**
     * Get an adapter by its id
     *
     * @return The adapter
     */
    @NotNull Optional<LogicalAdapter> getAdapter( long id );


    /*
     * Get list of all query interfaces
     *
     * @return List of query interfaces
     */
    @NotNull
    List<LogicalQueryInterface> getQueryInterfaces();

    /**
     * Get a query interface by its unique name
     *
     * @param uniqueName The unique name of the query interface
     * @return The CatalogQueryInterface
     */
    @NotNull
    Optional<LogicalQueryInterface> getQueryInterface( String uniqueName );

    @NotNull Optional<QueryInterfaceTemplate> getInterfaceTemplate( String name );



    List<LogicalTable> getTablesForPeriodicProcessing();

    //// OTHERS

    @Override
    default void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {

    }

    @Override
    default List<? extends Operator> getOperatorList() {
        return List.of();
    }


    Optional<AdapterTemplate> getAdapterTemplate( long templateId );

    @NotNull
    List<AdapterTemplate> getAdapterTemplates();

    @NotNull
    Optional<? extends LogicalEntity> getLogicalEntity( long id );

    @NotNull
    Optional<AdapterTemplate> getAdapterTemplate( String name, AdapterType adapterType );

    List<AdapterTemplate> getAdapterTemplates( AdapterType adapterType );

    List<QueryInterfaceTemplate> getInterfaceTemplates();


    LogicalRelSnapshot rel();

    LogicalGraphSnapshot graph();


    LogicalDocSnapshot doc();


    AllocSnapshot alloc();

    @NotNull
    Optional<LogicalEntity> getLogicalEntity( long namespaceId, String entity );

}
