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
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogNamespace;
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
import org.polypheny.db.catalog.logistic.Pattern;

public class FullSnapshot implements Snapshot {

    @Getter
    private final long id;


    public FullSnapshot( long id, Map<Long, LogicalCatalog> catalogs ) {
        this.id = id;


    }


    @Override
    public CatalogNamespace getNamespace( long id ) {
        return null;
    }


    @Override
    public CatalogNamespace getNamespace( String name ) {
        return null;
    }


    @Override
    public List<CatalogNamespace> getNamespaces( Pattern name ) {
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
