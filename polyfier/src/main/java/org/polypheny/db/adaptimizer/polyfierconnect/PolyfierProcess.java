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

package org.polypheny.db.adaptimizer.polyfierconnect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.ReAdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.rnddata.DataUtil;
import org.polypheny.db.adaptimizer.rndqueries.RelQueryTemplate;
import org.polypheny.db.adaptimizer.rndschema.DefaultTestEnvironment;
import org.polypheny.db.adaptimizer.rndschema.SchemaUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.io.Serializable;
import java.util.*;

@Slf4j
public class PolyfierProcess implements Runnable {
    public static final String SCHEMA_NAME = "adapt";

    private final Catalog catalog = Catalog.getInstance();
    private final DdlManager ddlManager = DdlManager.getInstance();
    private final AdapterManager adapterManager = AdapterManager.getInstance();
    private final TransactionManager transactionManager;
    private final PolyfierJob polyfierJob;

    public PolyfierProcess(TransactionManager transactionManager, PolyfierJob polyfierJob ) {
        this.transactionManager = transactionManager;
        this.polyfierJob = polyfierJob;
    }

    public static String testAutonomousPartitioning() {
        HashMap<String, String> datastores = new HashMap<>();
        datastores.put("HSQLDB", "true");
        datastores.put("PostgreSQL", "true");
        datastores.put("MonetDB", "true");
        datastores.put("Cottontail", "true");
        datastores.put("MongoDB", "false");
        datastores.put("Neo4j", "false"); // Todo add proper graphdb handling during partitioning
        datastores.put("Cassandra", "false"); // Todo cassandra really takes a long time to start
        /*
         *  java.lang.RuntimeException: The Docker container could not be started correctly.
         * 	at org.polypheny.db.docker.DockerInstance.waitTillStarted(DockerInstance.java:430) ~[core-0.8.2-SNAPSHOT.jar:?]
         * 	at org.polypheny.db.docker.DockerInstance.start(DockerInstance.java:355) ~[core-0.8.2-SNAPSHOT.jar:?]
         * 	at org.polypheny.db.docker.DockerManagerImpl.start(DockerManagerImpl.java:95) ~[core-0.8.2-SNAPSHOT.jar:?]
         * 	at org.polypheny.db.docker.DockerManager$Container.start(DockerManager.java:387) ~[core-0.8.2-SNAPSHOT.jar:?]
         */

        PolyfierJob polyfierJob = PolyfierJob.builder()
                .seed( 1337L )
                .datastores( datastores )
                .queries( 100 )
                .queryType( "DQL" )
                .configurations( null )
                .build();

        PolyfierProcess polyfierProcess = new PolyfierProcess( ReAdaptiveOptimizerImpl.getTransactionManager(), polyfierJob );

        polyfierProcess.run();

        return "Success";
    }


    private DataStore configureSchemaOnConfigDataStore() {
        long seed = this.polyfierJob.getSeed();


        // Todo Add Random Schemas
        log.debug("Generating Schema on configuration Store " + Adapter.HSQLDB.name() );
        return DefaultTestEnvironment.generate(Adapter.HSQLDB);
        //


    }

    private void generateEnvironmentData() {
        long seed = this.polyfierJob.getSeed();

        // Todo generate data for the random schema
        log.debug("Generating Data...");
        try {
            DataUtil.generateDataForDefaultEnvironment();
        } catch (UnknownColumnException e) {
            throw new RuntimeException(e);
        }
        //

    }


    public void run() {
        long seed = this.polyfierJob.getSeed();
        HashMap<String, String> stores = this.polyfierJob.getDatastores();

        // -----------------------------------------------------------------------
        log.debug("Initializing Datastores...");
        Transaction transaction;
        Statement statement;
        try {
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Polyfier" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not create transaction." );
        }

        List<DataStore> dataStores = new ArrayList<>();
        DataStore configDataStore = configureSchemaOnConfigDataStore(); // Configure Schema with graph/document/... based databases.. Namespaces?

        for ( String adapterKey : stores.keySet() ) {
            if ( ! Boolean.parseBoolean( stores.get( adapterKey ) ) ) {
                log.debug("Skipping Datastore: " + adapterKey );
                continue;
            }
            log.debug("Setting up Datastore: " + adapterKey );
            dataStores.add( SchemaUtil.addDataStore(Adapter.fromString(adapterKey)) );
        }

        // -----------------------------------------------------------------------
        log.debug("Partitioning Setup...");
        Random random = new Random( seed );

        List<CatalogTable> catalogTables;
        try {
            catalogTables = catalog.getTables( catalog.getSchema( Catalog.defaultDatabaseId, SCHEMA_NAME ).id, null );
        } catch (UnknownSchemaException e) {
            throw new RuntimeException(e);
        }

        // Here the partitioning could be setup differently...For now this works.

        final Stack<CatalogTable> tables = new Stack<>();
        catalogTables.forEach( tables::push );

        int j = -1;
        while ( ! tables.isEmpty() ) {
            ++j;
            if ( j < dataStores.size() && j < catalogTables.size() ) {
                try {
                    log.debug("Adding full data placement for Table " + tables.peek().name + " on " + dataStores.get(j).getAdapterName());
                    ddlManager.addDataPlacement(
                            tables.pop(),
                            List.of(),
                            List.of(),
                            List.of(),
                            dataStores.get(j),
                            statement
                    );
                } catch (PlacementAlreadyExistsException e) {
                    throw new RuntimeException(e);
                }
            } else if ( j < dataStores.size() ) {
                j = 0;
            } else {
                int m = random.nextInt(0, dataStores.size() );
                try {
                    log.debug("Adding full data placement for Table " + tables.peek().name + " on " + dataStores.get(m).getAdapterName() );
                    ddlManager.addDataPlacement(
                            tables.pop(),
                            List.of(),
                            List.of(),
                            List.of(),
                            dataStores.get( m ),
                            statement
                    );
                } catch (PlacementAlreadyExistsException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        try {
            transaction.commit();
        } catch (TransactionException e) {
            throw new RuntimeException(e);
        }

        // -----------------------------------------------------------------------
        log.debug("Removing Configuration Store...");

        try {
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Polyfier" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not create transaction." );
        }

        for ( CatalogTable table : catalogTables ) {
            try {
                ddlManager.dropDataPlacement( table, configDataStore, statement );
            } catch (PlacementNotExistsException | LastPlacementException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            transaction.commit();
        } catch (TransactionException e) {
            throw new RuntimeException(e);
        }
        adapterManager.removeAdapter( configDataStore.getAdapterId() );

        // -----------------------------------------------------------------------

        generateEnvironmentData();

        // -----------------------------------------------------------------------

        log.debug("Polyfier Session Setup Completed.");
        log.debug("Prepping Query Generation...");

        // -----------------------------------------------------------------------

        PolyfierDQLGenerator polyfierDQLGenerator = new PolyfierDQLGenerator(
                1337L,
                3,
                catalogTables,
                1,
                1,
                transactionManager
        );

        polyfierDQLGenerator.generate();


//        RelQueryTemplate relQueryTemplate = RelQueryTemplate.builder( catalog, catalogTables )
//                .schemaName( SCHEMA_NAME )
//                .addBinaryOperator( "Join", 5 )
//                .addBinaryOperator( "Union", 15 )
//                .addBinaryOperator( "Intersect", 7 )
//                .addBinaryOperator( "Minus", 3 )
//                .addUnaryOperator( "Sort", 15 )
//                .addUnaryOperator( "Project", 10 )
//                .addUnaryOperator( "Filter", 20 )
//                .addFilterOperator( "<>" )
//                .addJoinOperator( "=" )
//                .addJoinType( JoinAlgType.FULL )
//                .addJoinType( JoinAlgType.INNER )
//                .addJoinType( JoinAlgType.LEFT )
//                .addJoinType( JoinAlgType.RIGHT )
//                .unaryProbability( 0.5f )
//                .seed( seed )
//                .random( new Random( seed ) )
//                .height( 5 )
//                .build();
//
//        log.debug("Running Query Generation...");
//
//        ReAdaptiveOptimizerImpl adaptiveOptimizer = (ReAdaptiveOptimizerImpl) AdaptiveOptimizerImpl.getInstance();
//        String sid = adaptiveOptimizer.createSession( relQueryTemplate, 200 );
//        adaptiveOptimizer.joinStartedSession( sid );
//
//        log.debug("Completed Query Generation.");

    }

    private PolyfierResult from( long seed, AlgNode algNode ) {
        Transaction transaction;
        Statement statement;
        try {
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Polyfier" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not create transaction." );
        }

        PolyfierResult polyfierResult = new PolyfierQueryExecutor( statement, algNode, seed ).execute().getResult();

        if ( polyfierResult.wasSuccess() ) {
            try {
                transaction.commit();
            } catch (TransactionException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                transaction.rollback();
            } catch (TransactionException e) {
                throw new RuntimeException(e);
            }
        }
        return polyfierResult;
    }


    @Getter
    @Builder
    @AllArgsConstructor
    private static class PolyfierJob implements Serializable {
        private long seed;                          // Seed for generation.
        private int queries;                        // Number of queries.
        private String queryType;                   // DQL, DML.
        private HashMap<String, String> datastores;     // Involved Stores
        private HashMap<String, String> configurations; // Other Configurations.
    }

}
