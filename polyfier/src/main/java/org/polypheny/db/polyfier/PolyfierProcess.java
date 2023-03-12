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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class PolyfierProcess implements Runnable {
    private static final HttpClient HTTP_CLIENT = HttpClients.createDefault();
    private static final Gson GSON = new GsonBuilder().create();

    @Setter
    @Getter
    private static TransactionManager transactionManager;

    public static boolean testRun = false;

    public static final String SCHEMA_NAME = "adapt";
    private final Catalog catalog = Catalog.getInstance();
    private final DdlManager ddlManager = DdlManager.getInstance();
    private final AdapterManager adapterManager = AdapterManager.getInstance();
    private final PolyfierJob polyfierJob;
    private final String polyfierUrl;
    private final String apiKey;
    private final String orderKey;

    private PolyfierProcess(PolyfierJob polyfierJob, String polyfierUrl, String apiKey, String orderKey ) {
        this.polyfierJob = polyfierJob;
        this.polyfierUrl = polyfierUrl;
        this.apiKey = apiKey;
        this.orderKey = orderKey;
    }

    private static String executeHttpPost(String url, String requestBody) {
        try {
            HttpPost httpPost = new HttpPost(url);
            HttpEntity entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
            String responseBody;
            try (CloseableHttpResponse response = (CloseableHttpResponse) HTTP_CLIENT.execute(httpPost)) {
                responseBody = EntityUtils.toString(response.getEntity());
            }
            if (responseBody == null) {
                throw new RuntimeException("Server did not respond.");
            }
            return responseBody;
        } catch ( IOException ioException) {
            throw new RuntimeException("Could not contact server: ", ioException );
        }

    }

    public static void processPolyfierJob(String polyfierUrl, String apiKey, String orderKey) {
        String requestBody = "{'apiKey':" + apiKey + ",'clientId':" + orderKey + "}";
        String responseBody = executeHttpPost(polyfierUrl + "/request/polypheny-db/get-task", requestBody);
        PolyfierJob polyfierJob = GSON.fromJson(responseBody, PolyfierJob.class);
        PolyfierProcess polyfierProcess = new PolyfierProcess(polyfierJob, polyfierUrl, apiKey, orderKey);
        polyfierProcess.run();
    }

    private void sendResult(PolyfierResult polyfierResult) {
        polyfierResult.setApiKey(this.apiKey);
        polyfierResult.setOrderKey(this.orderKey);
        if ( log.isDebugEnabled() ) {
            log.debug("Result to send: " + GSON.toJson(polyfierResult));
        }
        String requestBody = GSON.toJson(polyfierResult);
        executeHttpPost(polyfierUrl + "/request/polypheny-db/result", requestBody);
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
                .queryConfig(new PolyfierJob.QueryConfig(null, null, 3))
                .dataConfig(new PolyfierJob.DataConfig(null))
                .partitionConfig(new PolyfierJob.PartitionConfig(null))
                .schemaConfig(new PolyfierJob.SchemaConfig("default"))
                .build();

        PolyfierProcess polyfierProcess = new PolyfierProcess(polyfierJob, null, null , null );

        polyfierProcess.run();

        return "Success";
    }


    private DataStore configureSchemaOnConfigDataStore() {
        PolyfierJob.SchemaConfig schemaConfig = this.polyfierJob.getSchemaConfig();

        String schemaType = schemaConfig.getSchema();

        // Todo add multiple schemas.
        if ( log.isDebugEnabled() ) {
            log.debug("Generating Schema on configuration Store HSQLDB");
        }
        return DefaultTestEnvironment.generate( Adapter.fromString("HSQLDB", CatalogAdapter.AdapterType.STORE ) );

    }

    private void generateEnvironmentData() {
        PolyfierJob.DataConfig dataConfig = this.polyfierJob.getDataConfig();
        HashMap<String, String> parameters = dataConfig.getParameters();

        // Todo add parameter based data generation.
        try {
            DataUtil.generateDataForDefaultEnvironment();
        } catch (UnknownColumnException e) {
            throw new RuntimeException(e);
        }
        //

    }

    private void addDataPlacement( Statement statement, CatalogTable table, DataStore dataStore) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Adding full data placement for Table " + table.name + " on " + dataStore.getAdapterName());
            }
            ddlManager.addDataPlacement(table, List.of(), List.of(), List.of(), dataStore, statement);
        } catch (PlacementAlreadyExistsException e) {
            throw new RuntimeException(e);
        }
    }

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


    public void run() {
        log.info("#".repeat(120));
        log.info(" ".repeat(46) + "POLYFIER PROCESS LAUNCHING");
        log.info("#".repeat(120));

        HashMap<String, String> stores = this.polyfierJob.getStoreConfig().getStores(); // True / False depending on whether store will be used.
        HashMap<String, String> storeParameters = this.polyfierJob.getStoreConfig().getParameters(); // Further configuration for stores.

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
        PolyfierJob.PartitionConfig partitionConfig = polyfierJob.getPartitionConfig();
        HashMap<String, String> partitionParameters = partitionConfig.getParameters();

        if (log.isDebugEnabled()) {
            log.debug("Partitioning Setup TMP...");
        }
        Random random = new Random(0);

        List<CatalogTable> catalogTables;
        try {
            catalogTables = catalog.getTables(catalog.getSchema(Catalog.defaultDatabaseId, SCHEMA_NAME).id, null);
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

        if ( log.isDebugEnabled() ) {
            log.debug("-".repeat(90));
            log.debug( "The Partition IDS / Partition Group IDS of the columns on the config Datastore are: " );
            log.debug("-".repeat(90));
            catalogTables.stream().map( table -> table.id ).map( catalog::getColumns ).forEach( columns -> columns.forEach( column -> {
                log.debug( String.format( "%-30s%-30s%-30s", "Table: " + column.getTableName(), "Column: " + column.name, "AdapterId: " + catalog.getColumnPlacement(column.id).stream().map(catalogColumnPlacement -> catalogColumnPlacement.adapterId).collect(Collectors.toList())));
            } ) );
            log.debug("-".repeat(90));
        }

        // -----------------------------------------------------------------------
        // Generating data on the Schema...

        log.info("Generating Data...");

        generateEnvironmentData();

        // -----------------------------------------------------------------------

        log.info("Polyfier Session Setup Completed.");
        log.info("Prepping Query Generation...");

        // -----------------------------------------------------------------------

        HashMap<String, ColumnStatistic> columnStatistics = new HashMap<>();

        // Retrieve relevant Statistical Information about the current Schema and data within it...
        log.info("Retrieving Statistical Information... ");
        for ( CatalogTable table : catalogTables ) {
            CatalogSchema catalogSchema = null;
            try {
                catalogSchema = catalog.getSchema( catalog.getDatabase( Catalog.defaultDatabaseId ).name , SCHEMA_NAME );
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
                    polyType = catalog.getColumn( table.id, column ).type;
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
        // Query Generation...

        // Todo add weights and parameters from server to the process...
        PolyfierJob.QueryConfig queryConfig = this.polyfierJob.getQueryConfig();
        HashMap<String, String> queryParameters = queryConfig.getParameters();
        HashMap<String, Double> decisionTreeWeights = queryConfig.getWeights();
        int nodes = queryConfig.getComplexity();

        Iterator<Long> seeds = this.polyfierJob.getSeedsConfig().iterator();

        if ( testRun ) {

            // Test A
            boolean runA = false;
            if ( runA ) {
                PolyfierJob.SeedsConfig seedsConfig = new PolyfierJob.SeedsConfig(
                        List.of( "0-1000") // Test range
                );

                List<Object> headers = List.of("complexity", "millis", "success", "failure", "blank-failure");
                Statistics.createCsvFile("test-run-a", headers );

                List<Object> data;
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 1 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 2 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 3 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 4 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 5 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 6 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 7 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 8 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 9 );
                Statistics.appendCsvFile( "test-run-a", data );
                data = testRunA( seedsConfig.iterator(), catalogTables, columnStatistics, 10 );
                Statistics.appendCsvFile( "test-run-a", data );
            }

            for ( int i = 1; i < 9; i++) {
                Statistics.createCsvFile("test-run-e" + i, List.of("Queries", "Unique"));
                testRunB( new PolyfierJob.SeedsConfig(List.of("0-100000") ).iterator(), catalogTables, columnStatistics, i );
            }


        } else {
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

    }


    private List<Object> testRunA( Iterator<Long> seeds, List<CatalogTable> catalogTables, HashMap<String, ColumnStatistic> columnStatistics, int nodes ) {
        final Integer[] counts = new Integer[3];
        counts[0] = 0;
        counts[1] = 0;
        counts[2] = 0;

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

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
                            res -> {
                                if ( res.wasSuccess() ) {
                                    counts[0] += 1;
                                } else {
                                    counts[1] += 1;
                                }
                            },
                            () -> {
                                counts[2] += 1;
                            }
                    );
        }

        stopWatch.stop();

        return List.of(nodes, stopWatch.getTime(TimeUnit.MILLISECONDS), counts[0], counts[1], counts[2]);
    }

    private void testRunB(  Iterator<Long> seeds, List<CatalogTable> catalogTables, HashMap<String, ColumnStatistic> columnStatistics, int nodes ) {
        Set<Long> planHashes = new HashSet<>();
        final ArrayList<Integer> queries = new ArrayList<>();
        final ArrayList<Integer> unique = new ArrayList<>();
        final int[] successQueries = new int[1];

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int i = 0;
        while ( seeds.hasNext() ) {
            Long seed = seeds.next();

            PolyfierDQLGenerator.builder()
                    .transactionManager( transactionManager )
                    .tables( catalogTables )
                    .columnStatistics( columnStatistics )
                    .nodes( nodes )
                    .seed( seed )
                    .build()
                    .testGenerate()
                    .ifPresentOrElse(
                            res -> {
                                successQueries[0]++;
                                planHashes.add( MurmurHash2.hash64( res ) );
                            },
                            () -> {
                                return;
                            }
                    );
            if ( successQueries[0] % 100 == 0 ) {
                i += 100;
                queries.add( i );
                unique.add( planHashes.size() );
            }
            if ( i % 1000 == 0 ) {
                log.info( "Count: " + successQueries[0] );
                transpose( queries, unique ).forEach(row -> {
                    Statistics.appendCsvFile("test-run-e" + nodes, row );
                });
                queries.clear();
                unique.clear();
            }
        }

        stopWatch.stop();

    }

    public static <T, U> List<List<Object>> transpose(List<T> list1, List<U> list2) {
        List<List<Object>> result = new ArrayList<>();
        Iterator<T> it1 = list1.iterator();
        Iterator<U> it2 = list2.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            List<Object> sublist = new ArrayList<>();
            sublist.add(it1.next());
            sublist.add(it2.next());
            result.add(sublist);
        }
        return result;
    }


}
