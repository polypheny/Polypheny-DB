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

package org.polypheny.db.backup.datagatherer;

import io.activej.serializer.annotations.Serialize;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.snapshot.impl.SnapshotBuilder;

import java.util.List;
import java.util.Map;


@Slf4j
public class GatherSchema {
    //gather the schemas from Polypheny-DB
    private final IdBuilder idBuilder = IdBuilder.getInstance();
    private Snapshot snapshot;

    private Catalog catalog = PolyCatalog.getInstance();

    public GatherSchema() {
    }

    public void start() {
        log.debug( "gather schemas" );

        //figure out how to get the snapshot from catalog bzw. how to create a new snapshot, and take infos out of it
        getSnapshot();
    }

    private void getSnapshot() {
        //get the snapshot from the catalog
        //this.snapshot = SnapshotBuilder.createSnapshot( idBuilder.getNewSnapshotId(), PolyCatalog.getInstance(), PolyCatalog.getInstance().logicalCatalogs, allocationCatalogs );

        this.snapshot = catalog.getSnapshot();
        int nbrNamespaces = snapshot.getNamespaces(null).size();
        //List<LogicalTable> tables = snapshot.getTablesForPeriodicProcessing();
        int publicTables = snapshot.rel().getTablesFromNamespace( 0 ).size();

        log.debug( "# namespaces = " + nbrNamespaces );
        log.debug( "# tables from public = " + publicTables );
    }
}
