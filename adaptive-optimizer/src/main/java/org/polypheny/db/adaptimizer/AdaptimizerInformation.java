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

package org.polypheny.db.adaptimizer;

import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.environment.DataGenerator;
import org.polypheny.db.adaptimizer.environment.DefaultTestEnvironment;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.information.GroupColor;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class AdaptimizerInformation {
    @Setter
    private static TransactionManager transactionManager;

    private static InformationManager informationManager;
    private static InformationPage page;

    public static void addInformationPage() {
        informationManager = InformationManager.getInstance();
        page = new InformationPage( "Adaptive Operator Cost Optimization",
                "This page displays information about the module ADAPTIMIZER. This includes random"
                        + "generation of operator trees, random generation of test data and runtime analysis of"
                        + "the module."
        ).setLabel( "ADAPT" );
        page.fullWidth();
        informationManager.addPage( page );
    }

    public static void addInformationGroupForTestDataGeneration() {
        InformationGroup informationGroup = new InformationGroup(page, "Random Record Generation");
        informationGroup.setColor( GroupColor.YELLOW );
        informationManager.addGroup( informationGroup );
        InformationText informationText = new InformationText( informationGroup, "Generate data for the default csv tables in public: ");
        InformationAction action = new InformationAction( informationGroup, "Run on default CSVs", parameters -> testDataGenerationCSV() );
        InformationAction action2 = new InformationAction( informationGroup, "Run on test table", parameters -> testDataGeneration() );
        informationGroup.addInformation(  informationText, action, action2 );
        informationManager.registerInformation( informationText, action, action2 );
        page.addGroup( informationGroup );
    }

    public static void addInformationGroupForGeneratingTestEnvironment() {
        InformationGroup informationGroup = new InformationGroup(page, "Generate Test Environment");
        informationGroup.setColor( GroupColor.GREEN );
        informationManager.addGroup( informationGroup );
        InformationText informationText = new InformationText( informationGroup, "Generate the default Test Environment: ");
        InformationAction action = new InformationAction( informationGroup, "Generate", parameters -> DefaultTestEnvironment.generate() );
        informationGroup.addInformation(  informationText, action );
        informationManager.registerInformation( informationText, action );
        page.addGroup( informationGroup );
    }

    private static String testDataGenerationCSV() {
        try {
            Catalog catalog = Catalog.getInstance();
            CatalogSchema schema = catalog.getSchema( catalog.getDatabase( Catalog.defaultDatabaseId ).name, "public" );
            CatalogTable depts = catalog.getTable( schema.id, "depts" );
            CatalogTable emps = catalog.getTable( schema.id, "emps" );
            CatalogTable emp = catalog.getTable( schema.id, "emp" );
            CatalogTable work = catalog.getTable( schema.id, "work" );

            DataGenerator testDataGenerator = new DataGenerator( transactionManager, List.of( depts, emps, emp, work ), List.of( 10, 10, 10, 10 ), 10 );
            testDataGenerator.generateData();
        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException e ) {
            log.error( "Data Generation Failed", e );
            return "Failed. " + e.getMessage();
        }
        return "Success.";
    }

    private static String testDataGeneration() {
        Catalog catalog = Catalog.getInstance();

        if ( DefaultTestEnvironment.CUSTOMERS_TABLE_ID == null ) {
            return "Test schema not generated...";
        }

        CatalogTable customersTable = catalog.getTable( DefaultTestEnvironment.CUSTOMERS_TABLE_ID );
        CatalogTable ordersTable = catalog.getTable( DefaultTestEnvironment.ORDERS_TABLE_ID );
        CatalogTable productsTable = catalog.getTable( DefaultTestEnvironment.PRODUCTS_TABLE_ID );
        CatalogTable shipmentsTable = catalog.getTable( DefaultTestEnvironment.SHIPMENTS_TABLE_ID );
        CatalogTable purchasesTable = catalog.getTable( DefaultTestEnvironment.PURCHASES_TABLE_ID );

        DataGenerator testDataGenerator = new DataGenerator(
                transactionManager,
                List.of( customersTable, ordersTable, productsTable, shipmentsTable, purchasesTable ),
                List.of( 1000, 2000, 100, 500, 20000 ), 100
        );
        testDataGenerator.generateData();
        return "Success.";
    }


}
