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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.backup.BackupInformationObject;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public class GatherSchema {

    //gather the schemas from Polypheny-DB
    private final IdBuilder idBuilder = IdBuilder.getInstance();
    private Snapshot snapshot;
    private BackupInformationObject backupInformationObject;

    private Catalog catalog = PolyCatalog.getInstance();

    //TODO(FF): don't safe data here, but safe it informationobject...
    //ImmutableMap<Long, LogicalNamespace> namespaces;
    List<LogicalNamespace> namespaces;
    List<LogicalNamespace> relNamespaces;
    List<LogicalNamespace> graphNamespaces;
    List<LogicalNamespace> docNamespaces;

    //namespace id, list of tables for the namespace
    ImmutableMap<Long, List<LogicalEntity>> tables;
    //TODO(FF): make views and materialized views not (? deleted?)... views and materialized views can be over several tables -> over several namespaces??
    ImmutableMap<Long, List<LogicalEntity>> views;
    ImmutableMap<Long, List<LogicalEntity>> materializedViews;
    ImmutableMap<Long, List<LogicalEntity>> collections;
    ImmutableMap<Long, LogicalEntity> graphs;

    //table id, list of views for the table
    ImmutableMap<Long, List<LogicalColumn>> columns;
    ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeysPerTable;
    ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable;
    //ImmutableMap<Long, List<LogicalKey>> keysPerTable;
    // uufspliite en pk, fk, constraints, indexes
    // index -> can only be created per (one) table
    ImmutableMap<Long, List<LogicalIndex>> logicalIndexes;
    //TODO(FF): if there exist constraint that go over several tables, need other way to signify it... rather use constraints per table, not per namespace! (but gets the right amount of constraints) --> constr only 1 table (?), views can be sever tables
    ImmutableMap<Long, List<LogicalConstraint>> constraints;

    Boolean collectedRel = false;
    Boolean collectedDoc = false;
    Boolean collectedGraph = false;


    public GatherSchema() {
    }


    public BackupInformationObject start( BackupInformationObject backupInformationObject ) {
        log.debug( "gather schemas" );
        this.backupInformationObject = backupInformationObject;

        //figure out how to get the snapshot from catalog bzw. how to create a new snapshot, and take infos out of it
        getSnapshot();
        getRelSchema();
        getDocSchema();
        getGraphSchema();
        testPrint();
        return backupInformationObject;
    }


    /**
     * Gets the snapshot from the catalog, and safes it in the class variable snapshot
     * Also safes list of all namespaces
     */
    private void getSnapshot() {

        this.snapshot = catalog.getSnapshot();
        int nbrNamespaces = snapshot.getNamespaces( null ).size();
        int publicTables = snapshot.rel().getTablesFromNamespace( 0 ).size();

        //this.namespaces = ImmutableMap.copyOf( namespaces );
        this.namespaces = snapshot.getNamespaces( null );
        this.backupInformationObject.setNamespaces( namespaces );

        log.debug( "# namespaces = " + nbrNamespaces );
        log.debug( "# tables from public = " + publicTables );
    }


    /**
     * Gets the tables, views, columns, keys, indexes, constraints and nodes from the snapshot
     */
    private void getRelSchema() {
        // TODO(FF): differentiate between views and materialized views (safe them seperately)
        Map<Long, List<LogicalTable>> tables = new HashMap<>();
        Map<Long, List<LogicalView>> views = new HashMap<>();
        Map<Long, List<LogicalMaterializedView>> materializedViews = new HashMap<>();
        Map<Long, List<LogicalColumn>> columns = new HashMap<>();
        Map<Long, List<LogicalConstraint>> constraints = new HashMap<>();
        //Map<Long, List<LogicalKey>> keysPerTable = new HashMap<>();
        Map<Long, List<LogicalPrimaryKey>> primaryKeysPerTable = new HashMap<>();
        Map<Long, List<LogicalForeignKey>> foreignKeysPerTable = new HashMap<>();
        Map<Long, List<LogicalIndex>> logicalIndex = new HashMap<>();
        //List<LogicalView> getConnectedViews( long id );

        List<LogicalNamespace> relNamespaces = namespaces.stream().filter( n -> n.namespaceType == NamespaceType.RELATIONAL ).collect( Collectors.toList() );
        this.relNamespaces = relNamespaces;
        this.backupInformationObject.setRelNamespaces( relNamespaces );

        // go through the list of namespaces and get the id of each namespace, map the tables to the namespace id
        //TODO(FF)?: views - list is just empty, but creates it nontheless, same for constraints, keys
        for ( LogicalNamespace namespace : relNamespaces ) {
            Long namespaceId = namespace.getId();

            // get tables from namespace
            List<LogicalTable> tablesFromNamespace = snapshot.rel().getTablesFromNamespace( namespaceId );
            //List<LogicalEntity> tablesFromNamespace = snapshot.rel().getTables( namespaceId, null ).stream().map( v -> v.unwrap( LogicalEntity.class ) ).collect(Collectors.toList( ));
            tables.put( namespaceId, tablesFromNamespace );

            // get other schema information for each table
            for ( LogicalTable table : tablesFromNamespace ) {
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

                //keys - (old: all keys selected: incl. pk, fk, constr, indexes
                //snapshot.rel().getKeys(); - get all keys
                //List<LogicalKey> tableKeys = snapshot.rel().getTableKeys( tableId );
                //keysPerTable.put( tableId, tableKeys );

                //primary keys (for the table)
                List<LogicalPrimaryKey> pkk = snapshot.rel().getPrimaryKeys().stream().filter( k -> k.tableId == tableId ).collect( Collectors.toList() );
                primaryKeysPerTable.put( tableId, pkk );

                // foreign keys
                List<LogicalForeignKey> fk = snapshot.rel().getForeignKeys( tableId );
                foreignKeysPerTable.put( tableId, fk );
                /*
                LogicalForeignKey(
	                name=fk_students_album,
	                referencedKeyId=0,
	                referencedKeySchemaId=0,
	                referencedKeyTableId=5,
	                updateRule=RESTRICT,
	                deleteRule=RESTRICT,
	                referencedKeyColumnIds=[26])
                 */

                //indexes
                List<LogicalIndex> logicalIdx = snapshot.rel().getIndexes( tableId, false );
                logicalIndex.put( tableId, logicalIdx );

                // get list of constraints for each table
                List<LogicalConstraint> tableConstraints = snapshot.rel().getConstraints( tableId );
                constraints.put( tableId, tableConstraints );


            }

        }

        //safes the gathered information in the class variables
        this.tables = ImmutableMap.copyOf( tables.entrySet().stream().collect(Collectors.toMap(v -> v.getKey(), v -> v.getValue().stream().map( e -> e.unwrap( LogicalEntity.class ) ).collect(Collectors.toList() ) )));
        //this.tables = ImmutableMap.copyOf( (Map<? extends Long, ? extends List<LogicalEntity>>) tables );
        //this.tables = ImmutableMap.copyOf( (Map<Long, ? extends List<LogicalEntity>>) tables );
        this.backupInformationObject.setTables( this.tables );
        this.views = ImmutableMap.copyOf( views.entrySet().stream().collect(Collectors.toMap(v -> v.getKey(), v -> v.getValue().stream().map( e -> e.unwrap( LogicalEntity.class ) ).collect(Collectors.toList() ) )) );
        this.backupInformationObject.setViews( this.views );
        this.columns = ImmutableMap.copyOf( columns );
        this.backupInformationObject.setColumns( this.columns );
        this.constraints = ImmutableMap.copyOf( constraints );
        this.backupInformationObject.setConstraints( this.constraints );
        //this.keysPerTable = ImmutableMap.copyOf( keysPerTable );
        this.primaryKeysPerTable = ImmutableMap.copyOf( primaryKeysPerTable );
        this.backupInformationObject.setPrimaryKeysPerTable( this.primaryKeysPerTable );
        this.foreignKeysPerTable = ImmutableMap.copyOf( foreignKeysPerTable );
        this.backupInformationObject.setForeignKeysPerTable( this.foreignKeysPerTable );
        this.logicalIndexes = ImmutableMap.copyOf( logicalIndex );
        this.backupInformationObject.setLogicalIndexes( this.logicalIndexes );

        this.backupInformationObject.setCollectedRelSchema( true );

    }


    /**
     * Gets the Graph schema from the snapshot, and safes it in class variables
     */
    private void getGraphSchema() {

        List<LogicalNamespace> graphNamespaces = namespaces.stream().filter( n -> n.namespaceType == NamespaceType.GRAPH ).collect( Collectors.toList() );
        this.graphNamespaces = graphNamespaces;
        this.backupInformationObject.setGraphNamespaces( graphNamespaces );

        List<LogicalGraph> graphsFromNamespace = snapshot.graph().getGraphs( null );

        //TODO(FF): can there only be one graph per namespace?
        Map<Long, LogicalGraph> nsGraphs = new HashMap<>();

        //for each graph get the namespaceId, see if it matches with the current namespace, and map the graph to the namespaceid
        for ( LogicalGraph graph : graphsFromNamespace ) {
            Long graphNsId = graph.getNamespaceId();

            // map the namespaceId to the graph
            nsGraphs.put( graphNsId, graph );
        }

        //safes the gathered information in the class variables
        this.graphs = ImmutableMap.copyOf( nsGraphs );
        this.backupInformationObject.setGraphs( this.graphs );
        this.backupInformationObject.setCollectedGraphSchema( true );

    }


    /**
     * Gets the Doc schema from the snapshot, and safes it in class variables
     */
    private void getDocSchema() {

        Map<Long, List<LogicalCollection>> nsCollections = new HashMap<>();
        List<LogicalNamespace> docNamespaces = namespaces.stream().filter( n -> n.namespaceType == NamespaceType.DOCUMENT ).collect( Collectors.toList() );
        this.docNamespaces = docNamespaces;
        this.backupInformationObject.setDocNamespaces( docNamespaces );

        for ( LogicalNamespace namespace : docNamespaces ) {
            Long namespaceId = namespace.getId();

            // get collections per namespace
            List<LogicalCollection> collectionsFromNamespace = snapshot.doc().getCollections( namespaceId, null );
            nsCollections.put( namespaceId, collectionsFromNamespace );
        }

        //safes the gathered information in the class variables
        this.collections = ImmutableMap.copyOf( nsCollections.entrySet().stream().collect(Collectors.toMap(v -> v.getKey(), v -> v.getValue().stream().map( e -> e.unwrap( LogicalEntity.class ) ).collect(Collectors.toList() ) )) );
        this.backupInformationObject.setCollections( this.collections );
        this.backupInformationObject.setCollectedDocSchema( true );
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
        log.debug( "primarykeysPerTable: " + primaryKeysPerTable.toString() );
        log.debug( "foreignkeysPerTable: " + foreignKeysPerTable.toString() );
        log.debug( "============================================= end print ==============================================" );
    }

}
