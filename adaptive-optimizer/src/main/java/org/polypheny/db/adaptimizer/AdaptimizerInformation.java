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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.PolyResult;
import org.polypheny.db.adaptimizer.randomdata.DataGenerator;
import org.polypheny.db.adaptimizer.randomdata.DataTableOptionTemplate;
import org.polypheny.db.adaptimizer.randomdata.TableOptionsBuilder;
import org.polypheny.db.adaptimizer.randomdata.except.TestDataGenerationException;
import org.polypheny.db.adaptimizer.randomschema.DefaultTestEnvironment;
import org.polypheny.db.adaptimizer.randomtrees.RelRandomTreeGenerator;
import org.polypheny.db.adaptimizer.randomtrees.RelRandomTreeTemplate;
import org.polypheny.db.adaptimizer.randomtrees.RelRandomTreeTemplates;
import org.polypheny.db.adaptimizer.randomtrees.TreeGenerator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

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

        InformationAction action2 = new InformationAction( informationGroup, "Run Tree Generator", parameters -> testTreeGenerator() );
        informationGroup.addInformation(  action2 );
        informationManager.registerInformation( action2 );

        InformationAction action3 = new InformationAction( informationGroup, "Try random Queries", parameters -> tryRandomQueries() );
        informationGroup.addInformation(  action3 );
        informationManager.registerInformation( action3 );

        InformationAction action4 = new InformationAction( informationGroup, "Test Seeding", parameters -> testSeeding() );
        informationGroup.addInformation(  action4 );
        informationManager.registerInformation( action4 );

        InformationAction action5 = new InformationAction( informationGroup, "Run Specific Tree", parameters -> runSpecificTrees() );
        informationGroup.addInformation(  action5 );
        informationManager.registerInformation( action5 );

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

        DataTableOptionTemplate customersTable = new TableOptionsBuilder( catalog, DefaultTestEnvironment.customers )
                .setSize( 1000 )
                .addLengthOption( "customer_name", 8 )
                .addLengthOption( "customer_phone", 10 )
                .addLengthOption( "customer_address", 20 )
                .build();
        DataTableOptionTemplate ordersTable = new TableOptionsBuilder( catalog, DefaultTestEnvironment.orders )
                .setSize( 2000 )
                .addTimestampRangeOption( "order_date", "2022-05-15 06:00:00", "2023-05-15 06:00:00")
                .build();
        DataTableOptionTemplate productsTable = new TableOptionsBuilder( catalog, DefaultTestEnvironment.products )
                .setSize( 100 )
                .addUniformValues( "product_name", List.of( "Chevrolet", "Hummer", "Fiat", "Peugot", "Farrari", "Nissan", "Porsche") )
                .addIntRangeOption( "product_stock", 0, 100 )
                .addDoubleRangeOption( "product_price", 5000, 1000000 )
                .build();
        DataTableOptionTemplate shipmentsTable = new TableOptionsBuilder( catalog, DefaultTestEnvironment.shipments )
                .setSize( 500 )
                .addDateRangeOption( "shipment_date", "2022-05-15", "2023-05-15" )
                .build();
        DataTableOptionTemplate purchasesTable = new TableOptionsBuilder( catalog, DefaultTestEnvironment.purchases )
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
        log.debug( "Generating Random Tree..." );
        Catalog catalog = Catalog.getInstance();
        Transaction transaction = getTransaction( catalog );
        RelRandomTreeTemplate relRandomTreeTemplate = RelRandomTreeTemplates.getRelRandomTreeTemplate( catalog );
        RelRandomTreeGenerator presetTreeGenerator = new RelRandomTreeGenerator( relRandomTreeTemplate );

        for ( int i = 0; i < 1000; i++ ) {
            Pair<AlgNode, Long> pair = presetTreeGenerator.generate( transaction.createStatement() );
        }

        return "Success";
    }

    private static String testTreeGenerator() {
        log.debug( "Testing Tree Generator..." );
        Catalog catalog = Catalog.getInstance();
        RelRandomTreeTemplate relRandomTreeTemplate = RelRandomTreeTemplates.getRelRandomTreeTemplate( catalog );

        TreeGenerator treeGenerator = new TreeGenerator( catalog, transactionManager, relRandomTreeTemplate );

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Stream.generate( treeGenerator ).limit( 10000 ).forEach( algNode -> {} );
        stopWatch.stop();

        log.debug( "STATS" );
        log.debug( "Time passed: {}ms", stopWatch.getTime() );
        log.debug( "{} total nodes generated", treeGenerator.getTreeNodeCounter() );
        log.debug( "{}ms average time per tree", treeGenerator.getAvgTime() );
        log.debug( "{}ms generation time", treeGenerator.getTime() );
        log.debug( "{}ms average time per success", treeGenerator.getTime() / treeGenerator.getSuccessCounter() );
        log.debug( "{} successes", treeGenerator.getSuccessCounter() );
        log.debug( "{} failures", treeGenerator.getFailureCounter() );
        log.debug( "{}% failures", treeGenerator.getFailureRate() );
        log.debug( "{}% successes", 100 - treeGenerator.getFailureRate() );

        return "Done.";
    }

    private static String runSpecificTrees() {

        Catalog catalog = Catalog.getInstance();

        // Get a template, in the RelRandomTreeTemplates put in your own templates to debug / try out.
        RelRandomTreeTemplate relRandomTreeTemplate = RelRandomTreeTemplates.getRelRandomTreeTemplate( catalog );

        // Make a tree generator
        TreeGenerator treeGenerator = new TreeGenerator( catalog, transactionManager, relRandomTreeTemplate );

        // set the seed      ->
        treeGenerator.setSeed( 142355L );

        // generate a tree
        Triple<Statement, AlgNode, Long> triple = treeGenerator.get();

        // print out debugging things e.g.
        log.debug( treeGenerator.getStringRep() );

        // Execute Tree
        try {
            executeTree( triple.getLeft(), triple.getMiddle() );
            treeGenerator.commitTransaction( false );
        } catch ( Exception e ) {
            log.debug( "Error", e );
            treeGenerator.rollbackTransaction( false );
        }

        return "Done.";
    }


    private static String tryRandomQueries() {
        log.debug( "Run random queries..." );
        Catalog catalog = Catalog.getInstance();

        // Get a template, in the RelRandomTreeTemplates put in your own templates to debug / try out.
        RelRandomTreeTemplate relRandomTreeTemplate = RelRandomTreeTemplates.getRelRandomTreeTemplate( catalog );

        TreeGenerator treeGenerator = new TreeGenerator( catalog, transactionManager, relRandomTreeTemplate );

        int nrOfExecutions = 1000;
        int errors = 0;
        for ( int i = 0; i < nrOfExecutions; i++ ) {

            Triple<Statement, AlgNode, Long> triple = treeGenerator.get();

            try {
                Thread.sleep( 5 );
            } catch ( InterruptedException e ) {
                // ignore
            }

            try {
                log.debug( "Executing Tree with seed:\n {}", triple.getRight() );
                List<List<Object>> result = executeTree( triple.getLeft(), triple.getMiddle() );
                log.debug( "\tSucceeded with result rows looking like this: {}", result.get( result.size() - 1 ) );
                treeGenerator.commitTransaction( i != nrOfExecutions - 1 );
            } catch ( RuntimeException re ) {
                log.debug( "\t Caught some runtime exception!" );
                ++errors;
                // Rolling back transaction here is instant block... Maybe it is the transactions causing the blocks not the trees...
                // treeGenerator.rollbackTransaction( i != nrOfExecutions - 1 );
            } catch ( Exception e ) {
                log.debug( "\t Caught some other exception!" );
                ++errors;
                treeGenerator.rollbackTransaction( i != nrOfExecutions - 1 );
            }

        }

        log.debug( "Executions: {}", nrOfExecutions );
        log.debug( "Errors: {}", errors );

        return "Done.";
    }

    private static String testSeeding() {
        log.debug( "testing seed coherence" );
        Catalog catalog = Catalog.getInstance();
        RelRandomTreeTemplate relRandomTreeTemplate = RelRandomTreeTemplates.getRelRandomTreeTemplate( catalog );
        TreeGenerator treeGenerator = new TreeGenerator( catalog, transactionManager, relRandomTreeTemplate );

        Stream.generate( treeGenerator ).limit( 5 ).forEach( tr -> log.debug( String.valueOf( tr.getRight() ) ) );
        Triple<Statement, AlgNode, Long> triple = treeGenerator.get();
        log.debug( treeGenerator.getStringRep() );
        log.debug( "Copying seed: " + triple.getRight() );
        Stream.generate( treeGenerator ).limit( 5 ).forEach( tr -> log.debug( String.valueOf( tr.getRight() ) ) );
        log.debug( "Resetting seed: " + triple.getRight() );
        treeGenerator.setSeed( triple.getRight() );
        log.debug( String.valueOf( treeGenerator.get().getRight() ) );
        log.debug( treeGenerator.getStringRep() );
        Stream.generate( treeGenerator ).limit( 5 ).forEach( tr -> log.debug( String.valueOf( tr.getRight() ) ) );

        return "Done.";
    }


    private static List<List<Object>> executeTree(Statement statement, AlgNode algNode ) {
        AlgRoot logicalRoot = AlgRoot.of( algNode, Kind.SELECT );
        PolyResult polyResult = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

        Iterator<Object> iterator = PolyResult.enumerable( polyResult.getBindable() , statement.getDataContext() ).iterator();
        try {
            return MetaImpl.collect( polyResult.getCursorFactory(), iterator, new ArrayList<>() );
        } catch ( Exception e ) {
            throw new TestDataGenerationException( "Could not execute insert query", e );
        }
    }


    private static Transaction getTransaction( Catalog catalog ) {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction(
                    catalog.getUser( Catalog.defaultUserId ).name,
                    catalog.getDatabase( Catalog.defaultDatabaseId ).name,
                    false,
                    "Adaptimizer Information"
            );
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            e.printStackTrace();
            throw new TestDataGenerationException( "Could not start transaction", e );
        }
        return transaction;
    }


}
