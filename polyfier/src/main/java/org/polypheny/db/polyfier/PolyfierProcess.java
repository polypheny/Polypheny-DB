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

package org.polypheny.db.polyfier;

import com.google.common.annotations.Beta;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.polyfier.core.PolyfierDQLGenerator;
import org.polypheny.db.polyfier.core.PolyfierJob;
import org.polypheny.db.polyfier.core.PolyfierResult;
import org.polypheny.db.polyfier.core.client.PolyfierClientPdb;
import org.polypheny.db.polyfier.core.client.profile.DataConfig;
import org.polypheny.db.polyfier.core.client.profile.Profile;
import org.polypheny.db.polyfier.core.construct.model.ColumnStatistic;
import org.polypheny.db.polyfier.data.DataUtil;
import org.polypheny.db.polyfier.schemas.DefaultTestEnvironment;
import org.polypheny.db.polyfier.schemas.SchemaUtil;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class PolyfierProcess {
    @Setter
    @Getter
    private static TransactionManager transactionManager;
    private static final Gson GSON = new GsonBuilder().create();
    public static final String SCHEMA_NAME = "adapt";
    private static final Catalog CATALOG = Catalog.getInstance();
    private static final DdlManager DDL_MANAGER = DdlManager.getInstance();
    private static final AdapterManager ADAPTER_MANAGER = AdapterManager.getInstance();
    private static PolyfierConfig polyfierConfig;

    private final PolyfierClientPdb polyfierClientPdb;
    public static boolean testRun = false;

    private PolyfierProcess( PolyfierConfig polyfierConfig ) {
        try {
            polyfierClientPdb = new PolyfierClientPdb(
                    new URI("ws://" + polyfierConfig.getUri() + "/ws"),
                    polyfierConfig.getApiKey(),
                    UUID.fromString( polyfierConfig.getPdbKey() )
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    @AllArgsConstructor
    private static class PolyfierConfig implements Serializable {
        private final String uri;
        private final String apiKey;
        private final String pdbKey;
    }

    public static void preparePolyfierProcess( String defaultPolyfierConfigPath ) {
        try {
            polyfierConfig = GSON.fromJson(
                    new JsonReader( new FileReader( defaultPolyfierConfigPath ) ),
                    PolyfierConfig.class
            );
            PolyfierProcess polyfierProcess = new PolyfierProcess( polyfierConfig );

            polyfierProcess.launch();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not read polyfier configuration from path: " + defaultPolyfierConfigPath, e);
        }
    }

    public static String testPolyfierProcess() {
        testRun = true;

        HashMap<String, String> dataStores = new HashMap<>();
        dataStores.put("HSQLDB", "true");
        dataStores.put("POSTGRESQL", "true");
        dataStores.put("MONETDB", "false");
        dataStores.put("COTTONTAIL", "false");
        dataStores.put("MONGODB", "false");
        dataStores.put("NEO4J", "false");
        dataStores.put("CASSANDRA", "false");

        PolyfierJob polyfierJob = PolyfierJob.builder()
                .storeConfig(new PolyfierJob.StoreConfig(null, dataStores))
                .seedsConfig(new PolyfierJob.SeedsConfig(List.of("0-1000")))
                .queryConfig(new PolyfierJob.QueryConfig(null, null, 8))
                .dataConfig(new PolyfierJob.DataConfig(null))
                .partitionConfig(new PolyfierJob.PartitionConfig(null))
                .schemaConfig(new PolyfierJob.SchemaConfig("default"))
                .build();

        // new PolyfierProcess( polyfierJob ).run();

        return "Success";
    }


    private DataStore configureSchemaOnConfigDataStore() {
        // Todo add multiple schemas.
        if ( log.isDebugEnabled() ) {
            log.debug("Generating Schema on configuration Store HSQLDB");
        }
        return DefaultTestEnvironment.generate( Adapter.fromString("HSQLDB", CatalogAdapter.AdapterType.STORE ) );
    }

    private void configureEnvironmentData( DataConfig dataConfig ) {
        Map<String, String> parameters = dataConfig.getParameters();

        // Todo add parameter based data generation. -> data module
        try {
            DataUtil.generateDataForDefaultEnvironment();
        } catch (UnknownColumnException e) {
            throw new RuntimeException(e);
        }
        //
    }

    @Beta
    private void addDataPlacement( Statement statement, CatalogTable table, DataStore dataStore) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Adding full data placement for Table " + table.name + " on " + dataStore.getAdapterName());
            }
            DDL_MANAGER.addDataPlacement(table, List.of(), List.of(), List.of(), dataStore, statement);
        } catch (PlacementAlreadyExistsException e) {
            throw new RuntimeException(e);
        }
    }

    @Beta
    private void placeTables( Statement statement, Stack<CatalogTable> tables, List<DataStore> dataStores, List<CatalogTable> catalogTables) {
        int j = -1;
        Random random = new Random( 0 );

        while (!tables.isEmpty()) {
            ++j;
            if (j < dataStores.size() && j < catalogTables.size()) {
                addDataPlacement( statement, tables.pop(), dataStores.get(j) );
            } else if (j < dataStores.size()) {
                j = 0;
            } else {
                int m = random.nextInt(0, dataStores.size());
                addDataPlacement( statement, tables.pop(), dataStores.get(m) );
            }
        }
    }


    public void launch() {
        log.info("#".repeat(120));
        log.info(" ".repeat(46) + "POLYFIER PROCESS LAUNCHING");
        log.info("#".repeat(120));

        polyfierClientPdb.changeStatus("START");

        Profile profile;
        try {
            profile = polyfierClientPdb.requestJob();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // True / False depending on whether store will be used.
        Map<String, String> stores = profile.getStoreConfig().getStores();
        // Further configuration for stores. // Unused
        Map<String, String> storeParameters = profile.getStoreConfig().getParameters();

        log.info("Initializing Schema...");

        // -----------------------------------------------------------------------
        if (log.isDebugEnabled()) {
            log.debug("Initializing Datastores...");
        }
        Transaction transaction;
        Statement statement;
        try {
            transaction = transactionManager.startTransaction(Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Polyfier");
            statement = transaction.createStatement();
        } catch (Exception e) {
            throw new RuntimeException("Could not create transaction.", e);
        }

        List<DataStore> dataStores = new ArrayList<>();
        DataStore configDataStore = configureSchemaOnConfigDataStore(); // Configure Schema with graph/document/... based databases.. Namespaces?

        if (log.isDebugEnabled()) {
            log.debug("The AdapterID of the Config-Store is: " + configDataStore.getAdapterId());
        }

        for (String adapterKey : stores.keySet()) {
            if (!Boolean.parseBoolean(stores.get(adapterKey))) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping Datastore: " + adapterKey);
                }
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("Setting up Datastore: " + adapterKey);
            }
            dataStores.add(SchemaUtil.addDataStore(Adapter.fromString(adapterKey, CatalogAdapter.AdapterType.STORE)));
        }

        // -----------------------------------------------------------------------

        // Todo exchange with partitioning configuration from config.
        Map<String, String> partitionParameters = profile.getPartitionConfig().getParameters();

        if (log.isDebugEnabled()) {
            log.debug("Partitioning Setup TMP...");
        }
        Random random = new Random(0);

        List<CatalogTable> catalogTables;
        try {
            catalogTables = CATALOG.getTables(CATALOG.getSchema(Catalog.defaultDatabaseId, SCHEMA_NAME).id, null);
        } catch (UnknownSchemaException e) {
            throw new RuntimeException(e);
        }

        final Stack<CatalogTable> tables = new Stack<>();
        catalogTables.forEach(tables::push);

        placeTables(statement, tables, dataStores, catalogTables);

        try {
            transaction.commit();
        } catch (TransactionException e) {
            throw new RuntimeException(e);
        }

        // -----------------------------------------------------------------------
        if (log.isDebugEnabled()) {
            log.debug("Removing Configuration Store...");
        }

        try {
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Polyfier" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not create transaction." );
        }

        for ( CatalogTable table : catalogTables ) {
            try {
                DDL_MANAGER.dropDataPlacement( table, configDataStore, statement );
            } catch (PlacementNotExistsException | LastPlacementException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            transaction.commit();
        } catch (TransactionException e) {
            throw new RuntimeException(e);
        }
        ADAPTER_MANAGER.removeAdapter( configDataStore.getAdapterId() );

        if ( log.isDebugEnabled() ) {
            log.debug("-".repeat(90));
            log.debug( "The Partition IDS / Partition Group IDS of the columns on the config Datastore are: " );
            log.debug("-".repeat(90));
            catalogTables.stream().map( table -> table.id ).map( CATALOG::getColumns ).forEach(columns -> columns.forEach(column -> {
                log.debug( String.format( "%-30s%-30s%-30s", "Table: " + column.getTableName(), "Column: " + column.name, "AdapterId: " + CATALOG.getColumnPlacement(column.id).stream().map(catalogColumnPlacement -> catalogColumnPlacement.adapterId).collect(Collectors.toList())));
            } ) );
            log.debug("-".repeat(90));
        }

        // -----------------------------------------------------------------------
        // Generating data on the Schema...

        log.info("Generating Data...");

        configureEnvironmentData( profile.getDataConfig() );

        // -----------------------------------------------------------------------

        log.info("Polyfier Session Setup Completed.");
        log.info("Prepping Query Generation...");

        // -----------------------------------------------------------------------

        // Todo capsule statistics in Statistics class
        HashMap<String, ColumnStatistic> columnStatistics = new HashMap<>();

        // Retrieve relevant Statistical Information about the current Schema and data within it...
        log.info("Retrieving Statistical Information... ");
        for ( CatalogTable table : catalogTables ) {
            CatalogSchema catalogSchema = null;
            try {
                catalogSchema = CATALOG.getSchema( CATALOG.getDatabase( Catalog.defaultDatabaseId ).name , SCHEMA_NAME );
            } catch (UnknownSchemaException | UnknownDatabaseException e) {
                throw new RuntimeException(e);
            }
            assert catalogSchema != null;

            try {
                transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Polyfier" );
                statement = transaction.createStatement();
            } catch ( Exception e ) {
                throw new RuntimeException( "Could not create transaction." );
            }


            long rowCount = Statistics.getRowCount( statement, SCHEMA_NAME + "." + table.name );


            if ( log.isDebugEnabled() ) {
                log.debug( "Querying >>" + SCHEMA_NAME + "." + table.name + " Rows: [" + rowCount + "] Columns: [" + table.getColumnNames().size() + "]" );

                log.debug("-".repeat(100));
                // We will query the value counts of all columns.
                log.debug(String.format("|  %-25s%-25s%-25s%-25s|", "FIELD", "DATATYPE", "AVG_VALUE_COUNT", "NUMERIC_AVG") );
                log.debug("=".repeat(100));
            }

            for ( String column : table.getColumnNames() ) {

                PolyType polyType = null;
                try {
                    polyType = CATALOG.getColumn( table.id, column ).type;
                } catch (UnknownColumnException e) {
                    throw new RuntimeException(e);
                }
                assert polyType != null;

                // These are the value counts... We should use them as a basis for filtering if they are clustered. Otherwise, if
                // entries are mostly unique we should look for aggregates or >=, <= queries, which are then executed in regard to
                // Mean, and variance of the data.
                List<List<Object>> valueCounts = Statistics.valueCounts( statement, SCHEMA_NAME + "." + table.name, column );

                try {
                    statement.getTransaction().commit();
                } catch (TransactionException e) {
                    throw new RuntimeException(e);
                }

                // This is the overall average value-count. If it's close to 1 we have mostly unique values. If it's high, we have clusters.
                double valueCountAverage = Statistics.valueCountAverage( valueCounts );

                double labelCountAverage = -1;
                if ( PolyType.NUMERIC_TYPES.contains( polyType  ) ) {

                    // Averaging, or doing other operations on the numeric types, gives us clues about their distribution, which we can use
                    // in le, lt, be, bt, operations during filtering.
                    labelCountAverage = Statistics.labelAverage( polyType, valueCounts );
                }

                if ( log.isDebugEnabled() ) {
                    log.debug(String.format("|  %-25s%-25s%-25s%-25s|", column, polyType, valueCountAverage, (labelCountAverage == -1) ? null : labelCountAverage ));
                }


                // If we have some values that occur frequently, sort them out and calculate the distribution.
                final List<Pair<Object, Double>> valueDist = new LinkedList<>();
                if ( valueCountAverage > 4 ) {
                    valueCounts
                            .stream()
                            .sorted( (obj1, obj2) -> Long.compare( (long) obj2.get( 1 ), (long) obj1.get( 1 ) ) )
                            .takeWhile( xs -> (long) xs.get( 1 ) / (float) rowCount > 0.05 ) // 5 % Threshold
                            .forEach( xs -> {
                                valueDist.add( Pair.of( String.valueOf( xs.get( 0 ) ),(long) xs.get( 1 ) / (double) rowCount ));
                            });

                    float other = 1f - (float) valueDist.stream().map( Pair::getValue ).mapToDouble( Double::valueOf ).sum();

                    if ( log.isDebugEnabled() ) {
                        log.debug(String.format("|  %-25s%-25s%-25s%-25s|", ">>>", ">>>", "VALUE", "FREQUENCY") );
                        valueDist.forEach( pair -> {
                            log.debug(String.format("|  %-25s%-25s%-25s%-25s|", "-", "-", pair.left, pair.right) );
                        });
                        log.debug(String.format("|  %-25s%-25s%-25s%-25s|", "-", "-", "Otherwise", other) );
                    }

                }

                columnStatistics.put(
                      column,
                      ColumnStatistic.builder()
                              .columnName( column )
                              .rowCount( rowCount )
                              .numericAverage( labelCountAverage )
                              .averageValueCount( valueCountAverage )
                              .type( polyType )
                              .frequency( valueDist )
                              .build()
                );

            }

            try {
                transaction.commit();
            } catch (TransactionException e) {
                throw new RuntimeException(e);
            }


        }


        log.info("Generating Queries...");
        polyfierClientPdb.changeStatus("BUSY");
        // Query Generation...

        // Todo add weights and parameters from server to the process...
        Map<String, String> queryParameters = profile.getQueryConfig().getParameters();
        Map<String, Double> decisionTreeWeights = profile.getQueryConfig().getWeights();
        int nodes = profile.getQueryConfig().getComplexity();

        Iterator<Long> seeds = profile.getIssuedSeeds().iter();


        while ( seeds.hasNext() ) {
            Long seed = seeds.next();
            PolyfierDQLGenerator.builder()
                .transactionManager( transactionManager )
                .tables( catalogTables )
                .columnStatistics( columnStatistics )
                .nodes( nodes )
                .seed( seed )
                .build()
                .generate()
                .ifPresentOrElse(
                        this::sendResult,
                        () -> this.sendResult( PolyfierResult.blankFailure( seed ) )
                );
        }

    }

    private void sendResult( PolyfierResult polyfierResult ) {
        polyfierClientPdb.depositResult(
                polyfierResult.getSeed(),
                polyfierResult.getSuccess(),
                polyfierResult.getResultSetHash(),
                polyfierResult.getError(),
                polyfierResult.getLogical(),
                polyfierResult.getPhysical(),
                polyfierResult.getActual()
        );
    }


}
