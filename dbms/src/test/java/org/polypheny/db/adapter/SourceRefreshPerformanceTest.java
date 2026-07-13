/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.ddl.DdlManager.SourceRefreshDetails;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.webui.Crud.SourceMaterializationRefreshResult;


@Tag("performance")
@SuppressWarnings("SqlDialectInspection")
class SourceRefreshPerformanceTest {

    private static final String SUFFIX = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
    private static final String USERNAME = "polypheny";
    private static final String PASSWORD = "polypheny";
    private static final int WARMUP_RUNS = 1;
    private static final int MEASURED_RUNS = 5;
    private static final int TOTAL_RUNS = WARMUP_RUNS + MEASURED_RUNS;
    private static final int[] RELATIONAL_ENTITY_SIZES = { 1_000, 10_000, 100_000 };
    private static final int[] DOCUMENT_ENTITY_SIZES = { 1_000, 10_000, 100_000 };
    private static final int[] SOURCE_ENTITY_COUNTS = { 100 };
    private static final int SOURCE_ROWS_PER_TABLE = 10_000;
    private static final int SOURCE_DOCUMENTS_PER_COLLECTION = 10_000;
    private static final Path REPORT = Path.of( "build", "reports", "source-refresh-performance.csv" );
    private static final String BENCHMARK_INSERT_COLUMNS = "id, customer_id, order_id, product_id, category_id, first_name, last_name, email, phone_number, street, city, country, postal_code, birth_date, created_at, updated_at, status, amount, is_active, notes";


    @Test
    void q1RelationalEntityRefreshDataOnly() throws Exception {
        assumePerformanceRun();
        for ( int rows : RELATIONAL_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q1", rows, run );
                String table = name( "q1_table", rows, run );
                String adapter = name( "pg_q1", rows, run );

                try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, USERNAME, PASSWORD ) ) {
                    postgres.execute( "CREATE TABLE public." + table + " (" + benchmarkTableDefinition( "q1_pk" ) + ")" );
                    insertRelationalRows( postgres, table, rows );
                    TestHelper.addPostgresSource( adapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD, "public." + table );
                    try {
                        long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, table, 30 ).id;
                        postgres.execute( "UPDATE public." + table + " SET category_id = category_id + 1" );

                        SourceMaterializationRefreshResult[] result = new SourceMaterializationRefreshResult[1];
                        long durationMs = measure( () -> result[0] = TestHelper.refreshRelationalSourceSchema( entityId ) );
                        assertNotNull( result[0] );
                        writeTiming( "Q1", "Relational entity refresh: schema refresh only after data change", "RELATIONAL", "entityRefresh", "schemaOnlyAfterDataChange", rows, 1, 0, rows, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q2RelationalEntityRefreshSimpleSchemaAddColumn() throws Exception {
        assumePerformanceRun();
        for ( int rows : RELATIONAL_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q2", rows, run );
                String table = name( "q2_table", rows, run );
                String adapter = name( "pg_q2", rows, run );

                try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, USERNAME, PASSWORD ) ) {
                    postgres.execute( "CREATE TABLE public." + table + " (" + benchmarkTableDefinition( "q2_pk" ) + ")" );
                    insertRelationalRows( postgres, table, rows );
                    TestHelper.addPostgresSource( adapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD, "public." + table );
                    try {
                        long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, table, 30 ).id;
                        postgres.execute( "ALTER TABLE public." + table + " ADD COLUMN loyalty_level VARCHAR(50)" );

                        SourceMaterializationRefreshResult[] result = new SourceMaterializationRefreshResult[1];
                        long durationMs = measure( () -> result[0] = TestHelper.refreshRelationalSourceSchema( entityId ) );
                        assertNotNull( result[0] );
                        assertHasColumn( entityId, "loyalty_level" );
                        writeTiming( "Q2", "Relational entity refresh: schema refresh only add column", "RELATIONAL", "entityRefresh", "schemaOnlyAddColumn", rows, 1, 0, rows, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q3RelationalEntityRefreshComplexSchemaAndData() throws Exception {
        assumePerformanceRun();
        for ( int rows : RELATIONAL_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q3", rows, run );
                String table = name( "q3_table", rows, run );
                String adapter = name( "pg_q3", rows, run );

                try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, USERNAME, PASSWORD ) ) {
                    postgres.execute( "CREATE TABLE public." + table + " (" + benchmarkTableDefinition( "q3_pk" ) + ")" );
                    insertRelationalRows( postgres, table, rows );
                    TestHelper.addPostgresSource( adapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD, "public." + table );
                    try {
                        long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, table, 30 ).id;
                        postgres.execute( "ALTER TABLE public." + table + " RENAME COLUMN first_name TO given_name" );
                        postgres.execute( "ALTER TABLE public." + table + " DROP CONSTRAINT q3_pk" );
                        postgres.execute( "ALTER TABLE public." + table + " ADD CONSTRAINT q3_pk PRIMARY KEY (customer_id)" );
                        postgres.execute( "ALTER TABLE public." + table + " ALTER COLUMN category_id TYPE BIGINT" );
                        postgres.execute( "ALTER TABLE public." + table + " ALTER COLUMN city SET NOT NULL" );
                        postgres.execute( "UPDATE public." + table + " SET category_id = category_id + 10" );

                        SourceMaterializationRefreshResult[] result = new SourceMaterializationRefreshResult[1];
                        long durationMs = measure( () -> result[0] = TestHelper.refreshRelationalSourceSchema( entityId ) );
                        assertNotNull( result[0] );
                        assertComplexRelationalSchema( entityId );
                        writeTiming( "Q3", "Relational entity refresh: schema refresh only complex schema", "RELATIONAL", "entityRefresh", "schemaOnlyComplexSchema", rows, 1, 0, rows, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q4RelationalSourceRefreshMixedChanges() throws Exception {
        assumePerformanceRun();
        for ( int tables : SOURCE_ENTITY_COUNTS ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q4", tables, run );
                String adapter = name( "pg_q4", tables, run );

                try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, USERNAME, PASSWORD ) ) {
                    for ( int i = 0; i < tables; i++ ) {
                        String table = sourceTableName( "q4_table", tables, run, i );
                        postgres.execute( "CREATE TABLE public." + table + " (" + benchmarkTableDefinition( "q4_" + i + "_pk" ) + ")" );
                        insertRelationalRows( postgres, table, SOURCE_ROWS_PER_TABLE );
                    }
                    TestHelper.addPostgresSource( adapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD );
                    try {
                        long sourceId = TestHelper.awaitSourceAdapterId( adapter, 30 );
                        for ( int i = 0; i < Math.min( 3, tables ); i++ ) {
                            String table = sourceTableName( "q4_table", tables, run, i );
                            postgres.execute( "ALTER TABLE public." + table + " ADD COLUMN loyalty_level VARCHAR(50)" );
                            postgres.execute( "ALTER TABLE public." + table + " ALTER COLUMN city SET NOT NULL" );
                        }
                        postgres.execute( "DROP TABLE public." + sourceTableName( "q4_table", tables, run, tables - 1 ) );
                        String addedTable = sourceTableName( "q4_added", tables, run, 0 );
                        String removedTable = sourceTableName( "q4_table", tables, run, tables - 1 );
                        postgres.execute( "CREATE TABLE public." + addedTable + " (" + benchmarkTableDefinition( "q4_added_pk" ) + ")" );
                        insertRelationalRows( postgres, addedTable, SOURCE_ROWS_PER_TABLE );

                        long durationMs = measure( () -> TestHelper.refreshSelectedSources( List.of( sourceId ) ) );
                        for ( int i = 0; i < Math.min( 3, tables ); i++ ) {
                            long refreshedEntityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceTableName( "q4_table", tables, run, i ), 30 ).id;
                            assertHasColumn( refreshedEntityId, "loyalty_level" );
                            assertColumnNullable( refreshedEntityId, "city", false );
                        }
                        TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, addedTable, 30 );
                        TestHelper.awaitLogicalTableAbsent( Catalog.defaultNamespaceId, removedTable, 30 );
                        writeTiming( "Q4", "Relational source refresh: mixed table changes", "RELATIONAL", "sourceRefresh", "mixedTables", SOURCE_ROWS_PER_TABLE, tables, 0, tables * SOURCE_ROWS_PER_TABLE, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q5DocumentSourceRefreshMixedCollectionChanges() throws Exception {
        assumePerformanceRun();
        for ( int collections : SOURCE_ENTITY_COUNTS ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q5", collections, run );
                String adapter = name( "mongo_q5", collections, run );

                try ( TestHelper.DockerMongo mongo = TestHelper.startMongoDocker( database ) ) {
                    for ( int i = 0; i < collections; i++ ) {
                        insertDocuments( mongo, sourceTableName( "q5_collection", collections, run, i ), SOURCE_DOCUMENTS_PER_COLLECTION );
                    }
                    TestHelper.addMongoSource( adapter, mongo.getHost(), mongo.getPort(), database );
                    try {
                        long sourceId = TestHelper.awaitSourceAdapterId( adapter, 30 );
                        long namespaceId = TestHelper.awaitDocumentNamespaceId( adapter, 30 );
                        for ( int i = 0; i < collections; i++ ) {
                            TestHelper.awaitLogicalCollection( namespaceId, sourceTableName( "q5_collection", collections, run, i ), 30 );
                        }
                        mongo.execute( "db." + sourceTableName( "q5_collection", collections, run, collections - 1 ) + ".drop()" );
                        String addedCollection = sourceTableName( "q5_added", collections, run, 0 );
                        insertDocuments( mongo, addedCollection, SOURCE_DOCUMENTS_PER_COLLECTION );

                        SourceRefreshDetails[] refresh = new SourceRefreshDetails[1];
                        long durationMs = measure( () -> refresh[0] = TestHelper.refreshSelectedSourcesWithDetails( List.of( sourceId ) ) );
                        assertSourceRefreshSummary( refresh[0], addedCollection, "Added source collection" );
                        assertSourceRefreshSummary( refresh[0], sourceTableName( "q5_collection", collections, run, collections - 1 ), "Removed source collection" );
                        TestHelper.awaitLogicalCollection( namespaceId, addedCollection, 30 );
                        TestHelper.awaitLogicalCollectionAbsent( namespaceId, sourceTableName( "q5_collection", collections, run, collections - 1 ), 30 );
                        writeTiming( "Q5", "Document source refresh: mixed collection changes", "DOCUMENT", "sourceRefresh", "mixedCollections", SOURCE_DOCUMENTS_PER_COLLECTION, 0, collections, collections * SOURCE_DOCUMENTS_PER_COLLECTION, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q6IndependentRelationalMaterializationCreation() throws Exception {
        assumePerformanceRun();
        for ( int rows : RELATIONAL_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q6", rows, run );
                String table = name( "q6_table", rows, run );
                String materializedTable = name( "q6_materialized", rows, run );
                String sourceAdapter = name( "pg_q6_source", rows, run );
                String storeAdapter = name( "pg_q6_store", rows, run );

                try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, USERNAME, PASSWORD ) ) {
                    postgres.execute( "CREATE TABLE public." + table + " (" + benchmarkTableDefinition( "q6_pk" ) + ")" );
                    insertRelationalRows( postgres, table, rows );
                    TestHelper.addPostgresSource( sourceAdapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD, "public." + table );
                    TestHelper.addPostgresStore( storeAdapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD );
                    try {
                        LogicalTable source = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, table, 30 );
                        long durationMs = measure( () -> TestHelper.createIndependentSourceMaterialization( source, materializedTable, storeAdapter ) );
                        assertEquals( rows, TestHelper.countRows( materializedTable ) );
                        writeTiming( "Q6", "Independent materialization creation relational", "RELATIONAL", "materializationCreate", "independent", rows, 1, 0, rows, run, durationMs );
                    } finally {
                        dropRelationalTableIfPresent( materializedTable );
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q7SynchronizedDocumentMaterializationCreation() throws Exception {
        assumePerformanceRun();
        for ( int documents : DOCUMENT_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q7", documents, run );
                String collection = name( "q7_collection", documents, run );
                String materializedCollection = name( "q7_materialized", documents, run );
                String sourceAdapter = name( "mongo_q7_source", documents, run );
                String storeAdapter = name( "mongo_q7_store", documents, run );

                try ( TestHelper.DockerMongo mongo = TestHelper.startMongoDocker( database ) ) {
                    insertDocuments( mongo, collection, documents );
                    TestHelper.addMongoSource( sourceAdapter, mongo.getHost(), mongo.getPort(), database );
                    TestHelper.addMongoStore( storeAdapter );
                    try {
                        long namespaceId = TestHelper.awaitDocumentNamespaceId( sourceAdapter, 30 );
                        LogicalCollection source = TestHelper.awaitLogicalCollection( namespaceId, collection, 30 );
                        String namespaceName = Catalog.snapshot().getNamespace( namespaceId ).orElseThrow().name;

                        long durationMs = measure( () -> TestHelper.createSynchronizedSourceCollectionMaterialization( source, materializedCollection, storeAdapter ) );
                        assertEquals( documents, TestHelper.countDocuments( namespaceName, materializedCollection ) );
                        writeTiming( "Q7", "Synchronized materialization creation document", "DOCUMENT", "materializationCreate", "synchronized", documents, 0, 1, documents, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
                    }
                }
            }
        }
    }


    @Test
    void q8SynchronizedRelationalMaterializationSchemaRefreshOnly() throws Exception {
        assumePerformanceRun();
        runSynchronizedRelationalMaterializationRefresh( "Q8", "schemaOnly", false );
    }


    @Test
    void q9SynchronizedRelationalMaterializationSchemaAndDataRefresh() throws Exception {
        assumePerformanceRun();
        runSynchronizedRelationalMaterializationRefresh( "Q9", "schemaAndData", true );
    }


    @Test
    void q10SynchronizedDocumentMaterializationRefresh() throws Exception {
        assumePerformanceRun();
        for ( int documents : DOCUMENT_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( "q10", documents, run );
                String collection = name( "q10_collection", documents, run );
                String materializedCollection = name( "q10_materialized", documents, run );
                String sourceAdapter = name( "mongo_q10_source", documents, run );
                String storeAdapter = name( "mongo_q10_store", documents, run );
                int documentsInsertedDuringRefresh = 5;

                try ( TestHelper.DockerMongo mongo = TestHelper.startMongoDocker( database ) ) {
                    insertDocuments( mongo, collection, documents - documentsInsertedDuringRefresh );
                    TestHelper.addMongoSource( sourceAdapter, mongo.getHost(), mongo.getPort(), database );
                    TestHelper.addMongoStore( storeAdapter );
                    try {
                        long namespaceId = TestHelper.awaitDocumentNamespaceId( sourceAdapter, 30 );
                        LogicalCollection source = TestHelper.awaitLogicalCollection( namespaceId, collection, 30 );
                        LogicalCollection materialization = TestHelper.createSynchronizedSourceCollectionMaterialization( source, materializedCollection, storeAdapter );
                        String namespaceName = Catalog.snapshot().getNamespace( namespaceId ).orElseThrow().name;
                        insertDocumentRange( mongo, collection, documents - documentsInsertedDuringRefresh + 1, documents );

                        long durationMs = measure( () -> TestHelper.refreshSynchronizedCollectionMaterializationData( materialization.id ) );
                        assertEquals( documents, TestHelper.countDocuments( namespaceName, materializedCollection ) );
                        writeTiming( "Q10", "Synchronized materialization refresh document", "DOCUMENT", "materializationRefresh", "data", documents, 0, 1, documents, run, durationMs );
                    } finally {
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
                    }
                }
            }
        }
    }


    private static void runSynchronizedRelationalMaterializationRefresh( String id, String changeType, boolean includeData ) throws Exception {
        for ( int rows : RELATIONAL_ENTITY_SIZES ) {
            for ( int run = 1; run <= TOTAL_RUNS; run++ ) {
                String database = databaseName( id.toLowerCase(), rows, run );
                String table = name( id.toLowerCase() + "_table", rows, run );
                String materializedTable = name( id.toLowerCase() + "_materialized", rows, run );
                String sourceAdapter = name( "pg_" + id.toLowerCase() + "_source", rows, run );
                String storeAdapter = name( "pg_" + id.toLowerCase() + "_store", rows, run );

                try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, USERNAME, PASSWORD ) ) {
                    postgres.execute( "CREATE TABLE public." + table + " (" + benchmarkTableDefinition( id.toLowerCase() + "_pk" ) + ")" );
                    insertRelationalRows( postgres, table, rows );
                    TestHelper.addPostgresSource( sourceAdapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD, "public." + table );
                    TestHelper.addPostgresStore( storeAdapter, postgres.getHost(), postgres.getPort(), database, USERNAME, PASSWORD );
                    try {
                        LogicalTable source = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, table, 30 );
                        LogicalTable materialization = TestHelper.createSynchronizedSourceMaterialization( source, materializedTable, storeAdapter );
                        postgres.execute( "ALTER TABLE public." + table + " RENAME COLUMN first_name TO given_name" );
                        postgres.execute( "ALTER TABLE public." + table + " ALTER COLUMN city SET NOT NULL" );
                        postgres.execute( "ALTER TABLE public." + table + " DROP CONSTRAINT " + id.toLowerCase() + "_pk" );
                        postgres.execute( "ALTER TABLE public." + table + " ADD CONSTRAINT " + id.toLowerCase() + "_pk PRIMARY KEY (customer_id)" );
                        postgres.execute( "ALTER TABLE public." + table + " ALTER COLUMN category_id TYPE BIGINT" );
                        if ( includeData ) {
                            postgres.execute( "UPDATE public." + table + " SET category_id = category_id + 1" );
                        }

                        long durationMs = includeData
                                ? measure( () -> TestHelper.refreshSynchronizedMaterializationData( materialization.id ) )
                                : measure( () -> TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id ) );
                        assertComplexRelationalSchema( materialization.id );
                        writeTiming( id, id.equals( "Q8" ) ? "Synchronized materialization refresh relational schema only" : "Synchronized materialization refresh relational schema and data", "RELATIONAL", "materializationRefresh", changeType, rows, 1, 0, rows, run, durationMs );
                    } finally {
                        dropRelationalTableIfPresent( materializedTable );
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                        TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
                    }
                }
            }
        }
    }


    private static void assumePerformanceRun() {
        Assumptions.assumeTrue(
                Boolean.getBoolean( "polypheny.refresh.performance" ) || Boolean.parseBoolean( System.getenv( "POLYPHENY_REFRESH_PERFORMANCE" ) ),
                "Enable with -Dpolypheny.refresh.performance=true or POLYPHENY_REFRESH_PERFORMANCE=true" );
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for source refresh performance tests" );
        TestHelper.getInstance();
    }


    private static long measure( ThrowingRunnable runnable ) throws Exception {
        long start = System.nanoTime();
        runnable.run();
        return (System.nanoTime() - start) / 1_000_000;
    }


    private static void assertSourceRefreshSummary( SourceRefreshDetails refresh, String entityName, String changeDescription ) {
        assertNotNull( refresh );
        assertTrue(
                refresh.summaries().stream().anyMatch( summary ->
                        summary.dataModel() == DataModel.DOCUMENT
                                && summary.entityName().equals( entityName )
                                && summary.changeDescriptions().contains( changeDescription ) ),
                "Expected source refresh summary for " + entityName + " containing '" + changeDescription + "'" );
    }


    private static void assertComplexRelationalSchema( long entityId ) {
        assertHasColumn( entityId, "given_name" );
        assertNoColumn( entityId, "first_name" );
        assertEquals( List.of( "customer_id" ), getPrimaryKeyColumnNames( entityId ) );
        assertColumnType( entityId, "category_id", PolyType.BIGINT );
        assertColumnNullable( entityId, "city", false );
    }


    private static void assertHasColumn( long entityId, String columnName ) {
        assertTrue( Catalog.snapshot().rel().getColumn( entityId, columnName ).isPresent(), "Expected column '" + columnName + "'" );
    }


    private static void assertNoColumn( long entityId, String columnName ) {
        assertFalse( Catalog.snapshot().rel().getColumn( entityId, columnName ).isPresent(), "Did not expect column '" + columnName + "'" );
    }


    private static void assertColumnNullable( long entityId, String columnName, boolean nullable ) {
        assertEquals( nullable, Catalog.snapshot().rel().getColumn( entityId, columnName ).orElseThrow().nullable, "Unexpected nullability for column '" + columnName + "'" );
    }


    private static void assertColumnType( long entityId, String columnName, PolyType type ) {
        assertEquals( type, Catalog.snapshot().rel().getColumn( entityId, columnName ).orElseThrow().type, "Unexpected type for column '" + columnName + "'" );
    }


    private static List<String> getPrimaryKeyColumnNames( long entityId ) {
        Long primaryKey = Catalog.snapshot().rel().getTable( entityId ).orElseThrow().primaryKey;
        if ( primaryKey == null ) {
            return List.of();
        }
        return Catalog.snapshot().rel().getPrimaryKey( primaryKey ).orElseThrow().fieldIds.stream()
                .map( id -> Catalog.snapshot().rel().getColumn( id ).orElseThrow().name )
                .toList();
    }


    private static String benchmarkTableDefinition( String primaryKeyName ) {
        return String.join( ", ",
                "id INTEGER NOT NULL",
                "customer_id INTEGER NOT NULL",
                "order_id INTEGER",
                "product_id INTEGER",
                "category_id INTEGER",
                "first_name VARCHAR(100)",
                "last_name VARCHAR(100)",
                "email VARCHAR(255)",
                "phone_number VARCHAR(30)",
                "street VARCHAR(255)",
                "city VARCHAR(100)",
                "country VARCHAR(100)",
                "postal_code VARCHAR(20)",
                "birth_date DATE",
                "created_at TIMESTAMP(3)",
                "updated_at TIMESTAMP(3)",
                "status VARCHAR(30)",
                "amount DECIMAL(10,2)",
                "is_active BOOLEAN",
                "notes TEXT",
                "CONSTRAINT " + primaryKeyName + " PRIMARY KEY (id)" );
    }


    private static void insertRelationalRows( TestHelper.DockerPostgres postgres, String table, int rows ) throws Exception {
        postgres.execute( "INSERT INTO public." + table + " (" + BENCHMARK_INSERT_COLUMNS + ") "
                + "SELECT gs, gs, gs, gs, gs, "
                + "'first_name_' || gs, "
                + "'last_name_' || gs, "
                + "'email_' || gs || '@example.com', "
                + "'phone_number_' || gs, "
                + "'street_' || gs, "
                + "'city_' || gs, "
                + "'country_' || gs, "
                + "'postal_code_' || gs, "
                + "DATE '1990-01-01', "
                + "TIMESTAMP '2024-01-01 12:00:00.123', "
                + "TIMESTAMP '2024-01-01 12:00:00.123', "
                + "'status_' || gs, "
                + "(gs + 0.10)::DECIMAL(10,2), "
                + "TRUE, "
                + "'notes_' || gs "
                + "FROM generate_series(1, " + rows + ") AS gs" );
    }


    private static void insertDocuments( TestHelper.DockerMongo mongo, String collection, int documents ) throws Exception {
        insertDocumentRange( mongo, collection, 1, documents );
    }


    private static void insertDocumentRange( TestHelper.DockerMongo mongo, String collection, int firstDocumentId, int lastDocumentId ) throws Exception {
        if ( lastDocumentId < firstDocumentId ) {
            return;
        }
        mongo.execute( """
                let bulk = db.%s.initializeUnorderedBulkOp();
                let pending = 0;
                for (let id = %d; id <= %d; id++) {
                  bulk.insert({
                    _id: id,
                    customer_id: id,
                    order_id: id,
                    product_id: id,
                    category_id: id,
                    first_name: 'first_name_' + id,
                    last_name: 'last_name_' + id,
                    email: 'email_' + id + '@example.com',
                    phone_number: 'phone_number_' + id,
                    street: 'street_' + id,
                    city: 'city_' + id,
                    country: 'country_' + id,
                    postal_code: 'postal_code_' + id,
                    birth_date: '1990-01-01',
                    created_at: '2024-01-01T12:00:00.123',
                    updated_at: '2024-01-01T12:00:00.123',
                    status: 'status_' + id,
                    amount: id + 0.10,
                    is_active: true,
                    notes: 'notes_' + id
                  });
                  pending++;
                  if (pending === 1000) {
                    bulk.execute();
                    bulk = db.%s.initializeUnorderedBulkOp();
                    pending = 0;
                  }
                }
                if (pending > 0) {
                  bulk.execute();
                }
                """.formatted( collection, firstDocumentId, lastDocumentId, collection ) );
    }


    private static void writeTiming(
            String id,
            String scenario,
            String dataModel,
            String operation,
            String changeType,
            int size,
            int tables,
            int collections,
            int records,
            int run,
            long durationMs ) throws Exception {
        if ( run <= WARMUP_RUNS ) {
            return;
        }

        int measuredRun = run - WARMUP_RUNS;
        Files.createDirectories( REPORT.getParent() );
        if ( Files.notExists( REPORT ) || Files.size( REPORT ) == 0 ) {
            Files.writeString( REPORT, "id,scenario,dataModel,operation,changeType,size,tables,collections,records,run,durationMs%n".formatted(), StandardOpenOption.CREATE, StandardOpenOption.APPEND );
        }
        Files.writeString(
                REPORT,
                "%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d%n".formatted( id, scenario, dataModel, operation, changeType, size, tables, collections, records, measuredRun, durationMs ),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND );
    }


    private static String databaseName( String scenario, int size, int run ) {
        return "perf_" + scenario + "_" + size + "_" + run + "_" + SUFFIX;
    }


    private static String name( String prefix, int size, int run ) {
        return prefix + "_" + size + "_" + run + "_" + SUFFIX;
    }


    private static String sourceTableName( String prefix, int size, int run, int index ) {
        return prefix + "_" + size + "_" + run + "_" + index + "_" + SUFFIX;
    }


    private static void dropRelationalTableIfPresent( String tableName ) throws Exception {
        if ( Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, tableName ).isPresent() ) {
            TestHelper.executeSQL( "DROP TABLE \"" + Catalog.DEFAULT_NAMESPACE_NAME + "\".\"" + tableName + "\"" );
        }
    }


    @FunctionalInterface
    private interface ThrowingRunnable {

        void run() throws Exception;

    }

}
