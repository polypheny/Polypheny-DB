/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;


@Slf4j
public class CatalogInfoPage implements PropertyChangeListener {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "HH:mm:ss" );

    private boolean addDebugMessages = RuntimeConfig.CATALOG_DEBUG_MESSAGES.getBoolean();

    private final InformationManager infoManager;
    private final Catalog catalog;
    private final InformationTable databaseInformation;
    private final InformationTable schemaInformation;
    private final InformationTable tableInformation;
    private final InformationTable columnInformation;
    private final InformationTable indexInformation;
    private final InformationTable adapterInformation;
    private final InformationTable partitionGroupInformation;
    private final InformationTable partitionInformation;

    private InformationTable debugInformation;


    public CatalogInfoPage( Catalog catalog ) {
        this.catalog = catalog;
        infoManager = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Catalog" );
        infoManager.addPage( page );

        this.adapterInformation = addCatalogInformationTable( page, "Adapters", 5, Arrays.asList( "ID", "Name", "Type" ), 0 );
        this.databaseInformation = addCatalogInformationTable( page, "Databases", 1, Arrays.asList( "ID", "Name", "Default SchemaID" ), 0 );
        this.schemaInformation = addCatalogInformationTable( page, "Schemas", 2, Arrays.asList( "ID", "Name", "DatabaseID", "SchemaType" ), 0 );
        this.tableInformation = addCatalogInformationTable( page, "Tables", 3, Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID", "Type", "PartitionType", "PartitionGroups" ), 0 );
        this.columnInformation = addCatalogInformationTable( page, "Columns", 4, Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID", "TableID", "Placements" ), 0 );
        this.indexInformation = addCatalogInformationTable( page, "Indexes", 6, Arrays.asList( "ID", "Name", "KeyID", "Location", "Method", "Unique" ), 0 );
        this.partitionGroupInformation = addCatalogInformationTable( page, "Partition Groups", 7, Arrays.asList( "ID", "Name", "TableID", "# Partitions" ), 0 );
        this.partitionInformation = addCatalogInformationTable( page, "Partitions", 8, Arrays.asList( "ID", "PartitionGroupID", "TableID", "Qualifiers" ), 0 );

        if ( addDebugMessages ) {
            debugInformation = addCatalogInformationTable( page, "Debug", 10, Arrays.asList( "Time", "Message" ), 100 );
        }
        addPersistentInfo( page );

        page.setRefreshFunction( this::resetCatalogInformation );
        catalog.addObserver( this );
        RuntimeConfig.CATALOG_DEBUG_MESSAGES.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                boolean newValue = RuntimeConfig.CATALOG_DEBUG_MESSAGES.getBoolean();
                if ( !addDebugMessages && newValue ) {
                    debugInformation = addCatalogInformationTable( page, "Debug", 10, Arrays.asList( "Time", "Message" ), 100 );
                } else if ( addDebugMessages && !newValue && debugInformation != null ) {
                    infoManager.removeInformation( debugInformation );
                    infoManager.removeGroup( infoManager.getGroup( debugInformation.getGroup() ) );
                    debugInformation = null;
                }
                addDebugMessages = newValue;
            }


            @Override
            public void restart( Config c ) {
                // do nothing
            }
        } );
    }


    private InformationTable addCatalogInformationTable( InformationPage page, String name, int order, List<String> titles, int limit ) {
        InformationGroup catalogGroup = new InformationGroup( page, name );
        catalogGroup.setOrder( order );
        infoManager.addGroup( catalogGroup );
        InformationTable table = new InformationTable( catalogGroup, titles, limit );
        infoManager.registerInformation( table );
        return table;
    }


    private void addPersistentInfo( InformationPage page ) {
        InformationGroup catalogGroup = new InformationGroup( page, "Persistency" );
        catalogGroup.setOrder( 9 );
        infoManager.addGroup( catalogGroup );
        InformationTable table = new InformationTable( catalogGroup, Collections.singletonList( "is persistent" ) );
        infoManager.registerInformation( table );
        table.addRow( catalog.isPersistent ? "✔" : "X" );
    }


    @Override
    public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        if ( addDebugMessages ) {
            addDebugMessage( propertyChangeEvent );
        }
    }


    private void addDebugMessage( PropertyChangeEvent propertyChangeEvent ) {
        String time = LocalTime.now().format( formatter );
        String msg = String.format(
                "[Property]: %s, [New]: %s, [Old]: %s",
                propertyChangeEvent.getPropertyName(),
                propertyChangeEvent.getNewValue(),
                propertyChangeEvent.getOldValue() );
        debugInformation.addRow( time, msg );
    }


    private void resetCatalogInformation() {
        databaseInformation.reset();
        schemaInformation.reset();
        tableInformation.reset();
        columnInformation.reset();
        adapterInformation.reset();
        indexInformation.reset();
        partitionGroupInformation.reset();
        partitionInformation.reset();

        if ( catalog == null ) {
            log.error( "Catalog not defined in the catalogInformationPage." );
            return;
        }
        try {
            catalog.getAdapters().forEach( s -> {
                adapterInformation.addRow( s.id, s.uniqueName, s.type );
            } );
            catalog.getDatabases( null ).forEach( d -> {
                databaseInformation.addRow( d.id, d.name, d.defaultSchemaId );
            } );
            catalog.getSchemas( null, null ).forEach( s -> {
                schemaInformation.addRow( s.id, s.name, s.databaseId, s.schemaType );
            } );
            catalog.getTables( null, null, null ).forEach( t -> {
                tableInformation.addRow( t.id, t.name, t.databaseId, t.schemaId, t.tableType, t.partitionProperty.partitionType.toString(), t.partitionProperty.partitionGroupIds.size() );
            } );
            catalog.getColumns( null, null, null, null ).forEach( c -> {
                String placements = catalog.getColumnPlacement( c.id ).stream().map( plac -> String.valueOf( plac.adapterId ) ).collect( Collectors.joining( "," ) );
                columnInformation.addRow( c.id, c.name, c.databaseId, c.schemaId, c.tableId, placements );
            } );
            catalog.getIndexes().forEach( i -> {
                indexInformation.addRow( i.id, i.name, i.keyId, i.location, i.method, i.unique );
            } );
            catalog.getPartitionGroups( null, null, null ).forEach( pg -> {
                partitionGroupInformation.addRow( pg.id, pg.partitionGroupName, pg.tableId, pg.partitionIds.size() );
            } );
            catalog.getPartitions( null, null, null ).forEach( p -> {
                partitionInformation.addRow( p.id, p.partitionGroupId, p.tableId, p.partitionQualifiers );
            } );
        } catch ( Exception e ) {
            log.error( "Exception while reset catalog information page", e );
        }
    }

}
