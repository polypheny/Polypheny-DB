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

import com.google.common.collect.ImmutableMap;
import io.activej.serializer.annotations.Serialize;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.snapshot.impl.SnapshotBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public class GatherSchema {
    //gather the schemas from Polypheny-DB
    private final IdBuilder idBuilder = IdBuilder.getInstance();
    private Snapshot snapshot;

    private Catalog catalog = PolyCatalog.getInstance();

    //ImmutableMap<Long, LogicalNamespace> namespaces;
    List<LogicalNamespace> namespaces;

    //namespace id, list of tables for the namespace
    ImmutableMap<Long, List<LogicalTable>> tables;
    //TODO(FF): if there exist constraint that go over several tables, need other way to signify it... rather use constraints per table, not per namespace! (but gets the right amount of constraints) --> ha sgfühl es get nome constraint öber 1 table
    ImmutableMap<Long, List<LogicalConstraint>> constraints;

    //table id, list of views for the table
    ImmutableMap<Long, List<LogicalView>> views;
    ImmutableMap<Long, List<LogicalMaterializedView>> materializedViews;
    ImmutableMap<Long, List<LogicalColumn>> columns;
    ImmutableMap<Long, List<LogicalKey>> keysPerTable;
    ImmutableMap<Long, List<LogicalIndex>> logicalIndexes;
    // index -> can only be created per (one) table

    public GatherSchema() {
    }

    public void start() {
        log.debug( "gather schemas" );

        //figure out how to get the snapshot from catalog bzw. how to create a new snapshot, and take infos out of it
        getSnapshot();
        getRelSchema();
        testPrint();
    }


    /**
     * Gets the snapshot from the catalog, and safes it in the class variable snapshot
     * Also safes list of all namespaces
     */
    private void getSnapshot() {

        this.snapshot = catalog.getSnapshot();
        int nbrNamespaces = snapshot.getNamespaces(null).size();
        int publicTables = snapshot.rel().getTablesFromNamespace( 0 ).size();

        //this.namespaces = ImmutableMap.copyOf( namespaces );
        this.namespaces = snapshot.getNamespaces( null );

        log.debug( "# namespaces = " + nbrNamespaces );
        log.debug( "# tables from public = " + publicTables );
    }

    /**
     * Gets the tables, views, columns, keys, indexes, constraints and nodes from the snapshot
     */
    private void getRelSchema() {
        //TODO(FF): differentiate between views and materialized views (safe them seperately)
        Map<Long, List<LogicalTable>> tables = new HashMap<>();
        Map<Long, List<LogicalView>> views = new HashMap<>();
        Map<Long, List<LogicalMaterializedView>> materializedViews = new HashMap<>();
        Map<Long, List<LogicalColumn>> columns = new HashMap<>();
        Map<Long, List<LogicalConstraint>> constraints = new HashMap<>();
        Map<Long, List<LogicalKey>> keysPerTable = new HashMap<>();
        Map<Long, List<LogicalIndex>> logicalIndex1 = new HashMap<>();
        Map<Long, List<LogicalIndex>> logicalIndex2 = new HashMap<>();
        //List<LogicalView> getConnectedViews( long id );

        List<LogicalNamespace> relNamespaces = namespaces.stream().filter( n -> n.namespaceType == NamespaceType.RELATIONAL ).collect( Collectors.toList() );

        // go through the list of namespaces and get the id of each namespace, map the tables to the namespace id
        //TODO(FF)?: views - list is just empty, but creates it nontheless, same for constraints, keys
        for (LogicalNamespace namespace : relNamespaces) {
            Long namespaceId = namespace.getId();

            // get tables from namespace
            List<LogicalTable> tablesFromNamespace = snapshot.rel().getTablesFromNamespace( namespaceId );
            tables.put( namespaceId, tablesFromNamespace );

            // get list of constraints for each namespace
            List<LogicalConstraint> tableConstraints = snapshot.rel().getConstraints();
            constraints.put( namespaceId, tableConstraints );

            // get other schema information for each table
            for (LogicalTable table : tablesFromNamespace) {
                Long tableId = table.getId();

                //views
                List<LogicalView> connectedViews = snapshot.rel().getConnectedViews( tableId );
                //TODO(FF): see if this actually works... (does it seperate correctly?) (views not handles yet correctly in snapshot)
                //get all materialized views from the list of views and materialized views
                List<LogicalMaterializedView> connMatView = connectedViews.stream().filter( v -> v instanceof LogicalMaterializedView ).map( v -> (LogicalMaterializedView) v ).collect( Collectors.toList() );
                //get all views from the list of views and materialized views
                List<LogicalView> connView = connectedViews.stream().filter( v -> v instanceof LogicalView ).map( v -> v ).collect( Collectors.toList() );
                //safes the views and materialized views in the maps
                views.put( tableId, connView );
                materializedViews.put( tableId, connMatView );

                //cols
                List<LogicalColumn> tableColumns = snapshot.rel().getColumns( tableId );
                columns.put( tableId, tableColumns );

                //keys
                //snapshot.rel().getKeys(); - get all keys
                List<LogicalKey> tableKeys = snapshot.rel().getTableKeys( tableId );
                keysPerTable.put( tableId, tableKeys );

                //indexes
                //TODO(FF): indexes are also safed as keys... (they are part of the keys list...) maybe i have to safe keys seperately as pks and fks... but am i missing keys? LogicalGenericKey, was cha das alles sii??
                // keyToIndexes = snapshot.rel().getKeys().stream().collect( Collectors.toMap( k -> k, k -> snapshot.rel().getIndexes( k ) ) );
                //keyToIndexes maps keyid to idx... (to know which key is an idx). Do i have to remove them from the key list? - see when building it to insert back? (for loop over all keys?) how do i get keyToIndexes map? havent figured out yet
                //TODO(FF): so pro table je die drü gliiche indexes... nope
                List<LogicalIndex> tableIndexes = snapshot.rel().getIndexes();
                logicalIndex1.put( tableId, tableIndexes );

                // Get the keyToIndexes map from the snapshot. maybe with the following?
                //TODO(FF): still need to test it, since intellij again didnt recognize map and list...
                //TODO(FF): yes this is the way: lischte met allne tables, aber nome bem rechtige table en lischte a indexes
                List<LogicalIndex> logicalIdx = snapshot.rel().getIndexes( tableId, false );
                logicalIndex2.put( tableId, logicalIdx );


            }

        }

        //safes the gathered information in the class variables
        this.tables = ImmutableMap.copyOf( tables );
        this.views = ImmutableMap.copyOf( views );
        this.columns = ImmutableMap.copyOf( columns );
        this.constraints = ImmutableMap.copyOf( constraints );
        this.keysPerTable = ImmutableMap.copyOf( keysPerTable );
        //for ( Long tableId : tables.keySet())

    }

    /**
     * Gets the Graph schema from the snapshot, and safes it in class variables
     */
    private void getGraphSchema() {


    }

    /**
     * Gets the Doc schema from the snapshot, and safes it in class variables
     */
    private void getDocSchema() {

    }

    //TODO (FF): either create getters (and setters?) or a "whole" getter class... to pass information to BackupManager


    /**
     * Prints some of the gathered information (in a debug statement)
     */
    private void testPrint() {
        log.debug( "============================================= test print ==============================================" );
        log.debug( "namespaces: " + namespaces.toString() );
        log.debug( "tables: " + tables.toString() );
        log.debug( "views: " + views.toString() );
        log.debug( "columns: " + columns.toString() );
        log.debug( "constraints: " + constraints.toString() );
        log.debug( "keysPerTable: " + keysPerTable.toString() );
        log.debug( "============================================= end print ==============================================" );
    }
}
