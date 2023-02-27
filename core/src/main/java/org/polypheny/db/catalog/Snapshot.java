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
import org.polypheny.db.catalog.entity.CatalogNamespace;
import org.polypheny.db.catalog.entity.LogicalCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.Pattern;

public interface Snapshot {

    CatalogNamespace getNamespace( long id );

    CatalogNamespace getNamespace( String name );

    List<CatalogNamespace> getNamespaces( Pattern name );

    //// ENTITIES

    LogicalTable getLogicalTable( long id );

    LogicalTable getLogicalTable( long namespaceId, String name );

    List<LogicalTable> getLogicalTables( long namespaceId, Pattern name );

    LogicalCollection getLogicalCollection( long id );

    LogicalCollection getLogicalCollection( long namespaceId, String name );

    List<LogicalCollection> getLogicalCollections( long namespaceId, Pattern name );

    LogicalGraph getLogicalGraph( long id );

    LogicalGraph getLogicalGraph( long namespaceId, String name );

    List<LogicalGraph> getLogicalGraphs( long namespaceId, Pattern name );

    AllocationTable getAllocTable( long id );

    AllocationCollection getAllocCollection( long id );

    AllocationGraph getAllocGraph( long id );

    PhysicalTable getPhysicalTable( long id );

    PhysicalCollection getPhysicalCollection( long id );

    PhysicalGraph getPhysicalGraph( long id );

}
