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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalRelationalCatalog;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;

@Value
public class FullSnapshot implements Snapshot {

    @Getter
    long id;
    PhysicalCatalog physicalCatalog;
    LogicalCatalog logicalCatalog;
    Map<Long, AllocationCatalog> allocationCatalogs;

    ImmutableMap<Long, LogicalNamespace> namespaceIds;

    ImmutableMap<String, LogicalNamespace> namespaceNames;

    ImmutableMap<Long, LogicalTable> tableIds;

    ImmutableMap<Pair<Long, String>, LogicalTable> tableNames;


    ImmutableMap<Long, LogicalColumn> columnIds;

    ImmutableMap<Triple<Long, Long, String>, LogicalColumn> columnNames;

    ImmutableMap<Long, LogicalCollection> collectionIds;
    ImmutableMap<Pair<Long, String>, LogicalCollection> collectionNames;

    ImmutableMap<Long, LogicalGraph> graphId;

    ImmutableMap<String, LogicalGraph> graphName;


    public FullSnapshot( long id, LogicalCatalog logicalCatalog, Map<Long, AllocationCatalog> allocationCatalogs, PhysicalCatalog physicalCatalog ) {
        this.id = id;
        this.logicalCatalog = logicalCatalog;
        this.allocationCatalogs = allocationCatalogs;
        this.physicalCatalog = physicalCatalog;

        namespaceIds = ImmutableMap.copyOf( logicalCatalogs.values().stream().map( LogicalCatalog::getLogicalNamespace ).collect( Collectors.toMap( n -> n.id, n -> n ) ) );
        namespaceNames = ImmutableMap.copyOf( namespaceIds.values().stream().collect( Collectors.toMap( n -> n.name, n -> n ) ) );

        tableIds = logicalCatalogs.values().stream()
                .filter( c -> c.getLogicalNamespace().namespaceType == NamespaceType.RELATIONAL )
                .map( c -> (LogicalRelationalCatalog) c ).flatMap( c -> c. )
    }


    @Override
    public LogicalNamespace getNamespace( long id ) {
        return null;
    }


    @Override
    public LogicalNamespace getNamespace( String name ) {
        return null;
    }


    @Override
    public List<LogicalNamespace> getNamespaces( Pattern name ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long id ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long namespaceId, String name ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( long namespaceId, Pattern name ) {
        return null;
    }


    @Override
    public CatalogEntity getEntity( List<String> names ) {
        return null;
    }


    @Override
    public LogicalTable getLogicalTable( List<String> names ) {
        return null;
    }


    @Override
    public LogicalCollection getLogicalCollection( List<String> names ) {
        return null;
    }


    @Override
    public LogicalGraph getLogicalGraph( List<String> names ) {
        return null;
    }


    @Override
    public LogicalTable getLogicalTable( long id ) {
        return null;
    }


    @Override
    public LogicalTable getLogicalTable( long namespaceId, String name ) {
        return null;
    }


    @Override
    public List<LogicalTable> getLogicalTables( long namespaceId, Pattern name ) {
        return null;
    }


    @Override
    public LogicalColumn getLogicalColumn( long id ) {
        return null;
    }


    @Override
    public LogicalCollection getLogicalCollection( long id ) {
        return null;
    }


    @Override
    public LogicalCollection getLogicalCollection( long namespaceId, String name ) {
        return null;
    }


    @Override
    public List<LogicalCollection> getLogicalCollections( long namespaceId, Pattern name ) {
        return null;
    }


    @Override
    public LogicalGraph getLogicalGraph( long id ) {
        return null;
    }


    @Override
    public LogicalGraph getLogicalGraph( long namespaceId, String name ) {
        return null;
    }


    @Override
    public List<LogicalGraph> getLogicalGraphs( long namespaceId, Pattern name ) {
        return null;
    }


    @Override
    public AllocationTable getAllocTable( long id ) {
        return null;
    }


    @Override
    public AllocationCollection getAllocCollection( long id ) {
        return null;
    }


    @Override
    public AllocationGraph getAllocGraph( long id ) {
        return null;
    }


    @Override
    public PhysicalTable getPhysicalTable( long id ) {
        return null;
    }


    @Override
    public PhysicalTable getPhysicalTable( long logicalId, long adapterId ) {
        return null;
    }


    @Override
    public PhysicalCollection getPhysicalCollection( long id ) {
        return null;
    }


    @Override
    public PhysicalCollection getPhysicalCollection( long logicalId, long adapterId ) {
        return null;
    }


    @Override
    public PhysicalGraph getPhysicalGraph( long id ) {
        return null;
    }


    @Override
    public PhysicalGraph getPhysicalGraph( long logicalId, long adapterId ) {
        return null;
    }


    @Override
    public boolean isPartitioned( long id ) {
        return false;
    }


    @Override
    public LogicalColumn getColumn( long columnId ) {
        return null;
    }

}
