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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
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

    //// NAMESPACES

    CatalogNamespace getNamespace( long id );

    CatalogNamespace getNamespace( String name );

    List<CatalogNamespace> getNamespaces( Pattern name );

    //// ENTITIES


    CatalogEntity getEntity( long id );

    CatalogEntity getEntity( long namespaceId, String name );

    CatalogEntity getEntity( long namespaceId, Pattern name );

    //// LOGICAL ENTITIES
    @Deprecated
    LogicalTable getLogicalTable( List<String> names );

    @Deprecated
    LogicalCollection getLogicalCollection( List<String> names );

    @Deprecated
    LogicalGraph getLogicalGraph( List<String> names );

    LogicalTable getLogicalTable( long id );

    LogicalTable getLogicalTable( long namespaceId, String name );

    List<LogicalTable> getLogicalTables( long namespaceId, Pattern name );

    LogicalCollection getLogicalCollection( long id );

    LogicalCollection getLogicalCollection( long namespaceId, String name );

    List<LogicalCollection> getLogicalCollections( long namespaceId, Pattern name );

    LogicalGraph getLogicalGraph( long id );

    LogicalGraph getLogicalGraph( long namespaceId, String name );

    List<LogicalGraph> getLogicalGraphs( long namespaceId, Pattern name );

    //// ALLOCATION ENTITIES

    AllocationTable getAllocTable( long id );

    AllocationCollection getAllocCollection( long id );

    AllocationGraph getAllocGraph( long id );

    //// PHYSICAL ENTITIES

    PhysicalTable getPhysicalTable( long id );

    PhysicalTable getPhysicalTable( long logicalId, long adapterId );

    PhysicalCollection getPhysicalCollection( long id );

    PhysicalCollection getPhysicalCollection( long logicalId, long adapterId );


    PhysicalGraph getPhysicalGraph( long id );

    PhysicalGraph getPhysicalGraph( long logicalId, long adapterId );

    //// LOGISTICS

    boolean isPartitioned( long id );

    //// OTHERS

    @Override
    default void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {

    }

    @Override
    default List<? extends Operator> getOperatorList() {
        return null;
    }

}
