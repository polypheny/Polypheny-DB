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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;

public interface Snapshot extends OperatorTable {

    NameMatcher nameMatcher = NameMatchers.withCaseSensitive( RuntimeConfig.RELATIONAL_CASE_SENSITIVE.getBoolean() );

    long getId();

    default Expression getSnapshotExpression( long id ) {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getSnapshot", Expressions.constant( id ) );
    }


    /**
     * Get all schemas which fit to the specified filter pattern.
     * <code>getNamespaces(xid, null, null)</code> returns all schemas of all databases.
     *
     * @param name Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @NonNull List<LogicalNamespace> getNamespaces( @Nullable Pattern name );

    /**
     * Returns the schema with the specified id.
     *
     * @param id The id of the schema
     * @return The schema
     */
    LogicalNamespace getNamespace( long id );

    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param name The name of the schema
     * @return The schema
     */
    LogicalNamespace getNamespace( String name );


    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param name The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     */
    boolean checkIfExistsNamespace( String name );


    /**
     * Get the user with the specified name
     *
     * @param name The name of the user
     * @return The user
     * @throws UnknownUserException If there is no user with the specified name
     */
    CatalogUser getUser( String name ) throws UnknownUserException;

    /**
     * Get the user with the specified id.
     *
     * @param id The id of the user
     * @return The user
     */
    CatalogUser getUser( long id );

    /**
     * Get list of all adapters
     *
     * @return List of adapters
     */
    List<CatalogAdapter> getAdapters();

    /**
     * Get an adapter by its unique name
     *
     * @return The adapter
     */
    CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException;

    /**
     * Get an adapter by its id
     *
     * @return The adapter
     */
    CatalogAdapter getAdapter( long id );

    /**
     * Check if an adapter with the given id exists
     *
     * @param id the id of the adapter
     * @return if the adapter exists
     */
    boolean checkIfExistsAdapter( long id );


    /*
     * Get list of all query interfaces
     *
     * @return List of query interfaces
     */
    List<CatalogQueryInterface> getQueryInterfaces();

    /**
     * Get a query interface by its unique name
     *
     * @param uniqueName The unique name of the query interface
     * @return The CatalogQueryInterface
     */
    CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException;

    /**
     * Get a query interface by its id
     *
     * @param id The id of the query interface
     * @return The CatalogQueryInterface
     */
    CatalogQueryInterface getQueryInterface( long id );


    List<LogicalTable> getTablesForPeriodicProcessing();

    //// ENTITIES

    CatalogEntity getEntity( long id );

    //// OTHERS

    @Override
    default void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {

    }

    @Override
    default List<? extends Operator> getOperatorList() {
        return List.of();
    }


    LogicalDocSnapshot getDocSnapshot( long namespaceId );

    LogicalGraphSnapshot getGraphSnapshot( long namespaceId );


    LogicalRelSnapshot getRelSnapshot( long namespaceId );


    PhysicalSnapshot getPhysicalSnapshot();

    AllocSnapshot getAllocSnapshot();


    List<CatalogIndex> getIndexes();

    LogicalEntity getLogicalEntity( long id );


}
