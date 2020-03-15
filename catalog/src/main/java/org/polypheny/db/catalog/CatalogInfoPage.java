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
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


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

        InformationGroup catalogGroup = new InformationGroup( page, "catalog" );
        infoManager.addGroup( catalogGroup );

        this.databaseInformation = new InformationTable( catalogGroup, Arrays.asList( "ID", "Name", "Default SchemaID" ) );

        infoManager.registerInformation( databaseInformation );

        this.schemaInformation = new InformationTable( catalogGroup, Arrays.asList( "ID", "Name", "DatabaseID", "SchemaType" ) );

        infoManager.registerInformation( schemaInformation );

        this.tableInformation = new InformationTable( catalogGroup, Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID" ) );

        infoManager.registerInformation( tableInformation );

        this.columnInformation = new InformationTable( catalogGroup, Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID", "TableID" ) );

        infoManager.registerInformation( columnInformation );

        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
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
                }, "Reset Min Max for all numericalColumns.",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS );


    }


    @Override
    public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        databaseInformation.reset();
        try {
            catalog.getDatabases( null ).forEach( d -> {
                databaseInformation.addRow( d.id, d.name, d.defaultSchemaId );
            } );
            catalog.getSchemas( null, null ).forEach( s -> {
                schemaInformation.addRow( s.id, s.name, s.databaseId, s.schemaType );
            } );

        } catch ( GenericCatalogException | UnknownSchemaException e ) {
            e.printStackTrace();
        }
    }
}
