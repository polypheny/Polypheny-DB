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

package org.polypheny.db.catalog.snapshot.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;

public class SnapshotBuilder {

    public static Snapshot createSnapshot( long id, Catalog catalog, Map<Long, LogicalCatalog> logicalCatalogs, Map<Long, AllocationCatalog> allocationCatalogs ) {
        LogicalRelSnapshot rels = buildRelSnapshots( logicalCatalogs );
        LogicalDocSnapshot docs = buildDocSnapshots( logicalCatalogs );
        LogicalGraphSnapshot graphs = buildGraphSnapshots( logicalCatalogs );

        AllocSnapshot alloc = buildAlloc( allocationCatalogs, catalog.getAdapters() );
        Map<Long, LogicalNamespace> namespaces = logicalCatalogs.entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> e.getValue().getLogicalNamespace() ) );

        return new SnapshotImpl( id, catalog, namespaces, rels, docs, graphs, alloc );
    }


    private static AllocSnapshot buildAlloc( Map<Long, AllocationCatalog> allocationCatalogs, Map<Long, LogicalAdapter> adapters ) {
        return new AllocSnapshotImpl( allocationCatalogs, adapters );
    }


    private static LogicalGraphSnapshot buildGraphSnapshots( Map<Long, LogicalCatalog> logicalCatalogs ) {
        return new LogicalGraphSnapshotImpl( logicalCatalogs
                .entrySet()
                .stream()
                .filter( e -> e.getValue().getLogicalNamespace().dataModel == DataModel.GRAPH )
                .collect( Collectors.toMap( Entry::getKey, e -> (LogicalGraphCatalog) e.getValue() ) ) );
    }


    private static LogicalDocSnapshot buildDocSnapshots( Map<Long, LogicalCatalog> logicalCatalogs ) {
        return new LogicalDocSnapshotImpl( logicalCatalogs
                .entrySet()
                .stream()
                .filter( e -> e.getValue().getLogicalNamespace().dataModel == DataModel.DOCUMENT )
                .collect( Collectors.toMap( Entry::getKey, e -> (LogicalDocumentCatalog) e.getValue() ) ) );
    }


    private static LogicalRelSnapshot buildRelSnapshots( Map<Long, LogicalCatalog> logicalCatalogs ) {
        return new LogicalRelSnapshotImpl( logicalCatalogs
                .entrySet()
                .stream()
                .filter( e -> e.getValue().getLogicalNamespace().dataModel == DataModel.RELATIONAL )
                .collect( Collectors.toMap( Entry::getKey, e -> (LogicalRelationalCatalog) e.getValue() ) ) );
    }

}
