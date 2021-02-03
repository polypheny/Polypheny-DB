/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;


@Slf4j
public class CatalogInfoPage implements PropertyChangeListener {

    private final InformationManager infoManager;
    private final Catalog catalog;
    private final InformationTable databaseInformation;
    private final InformationTable schemaInformation;
    private final InformationTable tableInformation;
    private final InformationTable columnInformation;
    private final InformationTable indexInformation;
    private final InformationTable adapterInformation;


    public CatalogInfoPage( Catalog catalog ) {
        this.catalog = catalog;
        infoManager = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Catalog" );
        infoManager.addPage( page );

        this.adapterInformation = addCatalogInformationTable( page, "Adapters", Arrays.asList( "ID", "Name", "Type" ) );
        this.databaseInformation = addCatalogInformationTable( page, "Databases", Arrays.asList( "ID", "Name", "Default SchemaID" ) );
        this.schemaInformation = addCatalogInformationTable( page, "Schemas", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaType" ) );
        this.tableInformation = addCatalogInformationTable( page, "Tables", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID" ) );
        this.columnInformation = addCatalogInformationTable( page, "Columns", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID", "TableID" ) );
        this.indexInformation = addCatalogInformationTable( page, "Indexes", Arrays.asList( "ID", "Name", "KeyID", "Location", "Method", "Unique" ) );

        addPersistentInfo( page );

        resetCatalogInformation();
        catalog.addObserver( this );
    }


    private InformationTable addCatalogInformationTable( InformationPage page, String name, List<String> titles ) {
        InformationGroup catalogGroup = new InformationGroup( page, name );
        infoManager.addGroup( catalogGroup );
        InformationTable table = new InformationTable( catalogGroup, titles );
        infoManager.registerInformation( table );
        return table;
    }


    private InformationTable addPersistentInfo( InformationPage page ) {
        InformationGroup catalogGroup = new InformationGroup( page, "Persistency" );
        infoManager.addGroup( catalogGroup );
        InformationTable table = new InformationTable( catalogGroup, Collections.singletonList( "is persistent" ) );
        infoManager.registerInformation( table );
        table.addRow( catalog.isPersistent ? "✔" : "X" );
        return table;
    }


    @Override
    public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        resetCatalogInformation();
    }


    private void resetCatalogInformation() {
        databaseInformation.reset();
        schemaInformation.reset();
        tableInformation.reset();
        columnInformation.reset();
        adapterInformation.reset();
        indexInformation.reset();
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
                tableInformation.addRow( t.id, t.name, t.databaseId, t.schemaId );
            } );
            catalog.getColumns( null, null, null, null ).forEach( c -> {
                columnInformation.addRow( c.id, c.name, c.databaseId, c.schemaId, c.tableId );
            } );
            catalog.getIndexes().forEach( i -> {
                indexInformation.addRow( i.id, i.name, i.keyId, i.location, i.method, i.unique );
            } );
        } catch ( Exception e ) {
            log.error( "Exception while reset catalog information page", e );
        }
    }

}
