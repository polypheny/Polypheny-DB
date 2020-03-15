/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.List;
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;


public class CatalogInfoPage implements PropertyChangeListener {

    private final InformationManager infoManager;
    private final Catalog catalog;
    private final InformationTable databaseInformation;
    private final InformationTable schemaInformation;
    private final InformationTable tableInformation;
    private final InformationTable columnInformation;


    public CatalogInfoPage( Catalog catalog ) {
        this.catalog = catalog;
        infoManager = InformationManager.getInstance();

        InformationPage page = new InformationPage( "catalog", "Catalog" );
        infoManager.addPage( page );

        this.databaseInformation = addCatalogInformationTable( page, "databases", Arrays.asList( "ID", "Name", "Default SchemaID" ) );

        this.schemaInformation = addCatalogInformationTable( page, "schemas", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaType" ) );

        this.tableInformation = addCatalogInformationTable( page, "tables", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID" ) );

        this.columnInformation = addCatalogInformationTable( page, "columns", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID", "TableID" ) );

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


    @Override
    public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        resetCatalogInformation();
    }


    private void resetCatalogInformation() {
        databaseInformation.reset();
        schemaInformation.reset();
        tableInformation.reset();
        columnInformation.reset();
        try {
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

        } catch ( GenericCatalogException | UnknownSchemaException | UnknownCollationException | UnknownColumnException | UnknownTypeException | UnknownTableException e ) {
            e.printStackTrace();
        }
    }
}
