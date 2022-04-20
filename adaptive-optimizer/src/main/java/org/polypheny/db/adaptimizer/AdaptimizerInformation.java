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
import org.polypheny.db.adaptimizer.environment.DataTableOptionTemplate;
import org.polypheny.db.adaptimizer.environment.DefaultTestEnvironment;
import org.polypheny.db.adaptimizer.environment.TableOptionsBuilder;
import org.polypheny.db.adaptimizer.except.TestDataGenerationException;
import org.polypheny.db.adaptimizer.randomtrees.RelRandomTreeGenerator;
import org.polypheny.db.adaptimizer.randomtrees.RandomTreeTemplateBuilder;
import org.polypheny.db.adaptimizer.randomtrees.RelRandomTreeTemplate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class AdaptimizerInformation {
    @Setter
    private static TransactionManager transactionManager;

    private static InformationManager informationManager;
    private static InformationPage page;
    private static InformationGroup testDataGenGroup;
    private static InformationGroup testEnvGenGroup;
    private static InformationGroup randomTreeGenGroup;

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

    public static void addInformationGroupForRandomTreeGeneration() {
        InformationGroup informationGroup = new InformationGroup(page, "Random Tree Generation");
        informationManager.addGroup( informationGroup );
        InformationAction action = new InformationAction( informationGroup, "Generate Random Tree on Test Data", parameters -> generateRandomTree() );
        informationGroup.addInformation(  action );
        informationManager.registerInformation( action );
        page.addGroup( informationGroup );
        randomTreeGenGroup = informationGroup;
    }

    public static void addInformationGroupForTestDataGeneration() {
        InformationGroup informationGroup = new InformationGroup(page, "Random Record Generation");
        informationManager.addGroup( informationGroup );
        InformationAction action = new InformationAction( informationGroup, "Fill Default Test Schema with Dummy Data", parameters -> generateTestData() );
        informationGroup.addInformation(  action );
        informationManager.registerInformation( action );
        page.addGroup( informationGroup );
        testDataGenGroup = informationGroup;
    }

    public static void addInformationGroupForGeneratingTestEnvironment() {
        InformationGroup informationGroup = new InformationGroup(page, "Generate Test Environment");
        informationManager.addGroup( informationGroup );
        InformationAction action = new InformationAction( informationGroup, "Generate Schema & Tables", parameters -> DefaultTestEnvironment.generate() );
        informationGroup.addInformation( action );
        informationManager.registerInformation( action );
        page.addGroup( informationGroup );
        testEnvGenGroup = informationGroup;
    }


    private static String generateTestData() {
        Catalog catalog = Catalog.getInstance();

        if ( DefaultTestEnvironment.CUSTOMERS_TABLE_ID == null ) {
            return "Test schema not generated...";
        }
        DataTableOptionTemplate customersTable = new TableOptionsBuilder( catalog, catalog.getTable( DefaultTestEnvironment.CUSTOMERS_TABLE_ID ) )
                .setSize( 1000 )
                .addLengthOption( "customer_name", 8 )
                .addLengthOption( "customer_phone", 10 )
                .addLengthOption( "customer_address", 20 )
                .build();
        DataTableOptionTemplate ordersTable = new TableOptionsBuilder( catalog, catalog.getTable( DefaultTestEnvironment.ORDERS_TABLE_ID ) )
                .setSize( 2000 )
                .addTimestampRangeOption( "order_date", "2022-05-15 06:00:00", "2023-05-15 06:00:00")
                .build();
        DataTableOptionTemplate productsTable = new TableOptionsBuilder( catalog, catalog.getTable( DefaultTestEnvironment.PRODUCTS_TABLE_ID ) )
                .setSize( 100 )
                .addUniformValues( "product_name", List.of( "Chevrolet", "Hummer", "Fiat", "Peugot", "Farrari", "Nissan", "Porsche") )
                .addIntRangeOption( "product_stock", 0, 100 )
                .addDoubleRangeOption( "product_price", 5000, 1000000 )
                .build();
        DataTableOptionTemplate shipmentsTable = new TableOptionsBuilder( catalog, catalog.getTable( DefaultTestEnvironment.SHIPMENTS_TABLE_ID ) )
                .setSize( 500 )
                .addDateRangeOption( "shipment_date", "2022-05-15", "2023-05-15" )
                .build();
        DataTableOptionTemplate purchasesTable = new TableOptionsBuilder( catalog, catalog.getTable( DefaultTestEnvironment.PURCHASES_TABLE_ID ) )
                .setSize( 20000 )
                .addIntRangeOption( "quantity", 1, 10 )
                .build();

        DataGenerator testDataGenerator = new DataGenerator(
                transactionManager,
                List.of( customersTable, ordersTable, productsTable, shipmentsTable, purchasesTable ),
                100
        );
        testDataGenerator.generateData();
        return "Success.";
    }


    private static String generateRandomTree() {
        Catalog catalog = Catalog.getInstance();
        Transaction transaction = getTransaction( catalog );
        RelRandomTreeTemplate relRandomTreeTemplate = new RandomTreeTemplateBuilder( catalog )
                .addTable( catalog.getTable( DefaultTestEnvironment.CUSTOMERS_TABLE_ID ) )
                .addTable(  catalog.getTable( DefaultTestEnvironment.ORDERS_TABLE_ID ) )
                .addTable( catalog.getTable( DefaultTestEnvironment.PRODUCTS_TABLE_ID )  )
                .addTable( catalog.getTable( DefaultTestEnvironment.PURCHASES_TABLE_ID ) )
                .addTable( catalog.getTable( DefaultTestEnvironment.SHIPMENTS_TABLE_ID ) )
                .addOperator( "Join", 10 )
                .addOperator( "Sort", 20 )
                .addOperator( "Project", 20 )
                .addOperator( "Filter", 10 )
                .setSchemaName( DefaultTestEnvironment.SCHEMA_NAME )
                .setMaxHeight( 10 )
                .build();
        RelRandomTreeGenerator presetTreeGenerator = new RelRandomTreeGenerator( relRandomTreeTemplate );
        AlgNode node = presetTreeGenerator.generate( transaction.createStatement() );
        return "Success";
    }



    private static Transaction getTransaction( Catalog catalog ) {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction(
                    catalog.getUser( Catalog.defaultUserId ).name,
                    catalog.getDatabase( Catalog.defaultDatabaseId ).name,
                    false,
                    null
            );
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            e.printStackTrace();
            throw new TestDataGenerationException( "Could not start transaction", e );
        }
        return transaction;
    }


}
