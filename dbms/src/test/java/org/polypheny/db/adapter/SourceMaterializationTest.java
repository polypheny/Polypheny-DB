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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.type.PolyType;


@Tag("adapter")
@SuppressWarnings("SqlDialectInspection")
class SourceMaterializationTest {

    private static final String SUFFIX = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
    private static final String POSTGRES_DATABASE = "polypheny_mat_refresh_" + SUFFIX;
    private static final String POSTGRES_USERNAME = "polypheny";
    private static final String POSTGRES_PASSWORD = "polypheny";


    @Test
    void createsIndependentSourceMaterialization() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String sourceTable = "mat_ind_source_" + SUFFIX;
        String materializedTable = "mat_ind_target_" + SUFFIX;
        String sourceAdapter = "pg_mat_ind_source_" + SUFFIX;
        String storeAdapter = "pg_mat_ind_store_" + SUFFIX;

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + sourceTable + " (id INTEGER PRIMARY KEY, name VARCHAR(100))" );
            postgres.execute( "INSERT INTO public." + sourceTable + " (id, name) VALUES (1, 'Alice'), (2, 'Bob')" );
            TestHelper.addPostgresSource( sourceAdapter, postgres.getHost(), postgres.getPort(), POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, "public." + sourceTable );
            TestHelper.addPostgresStore( storeAdapter, postgres.getHost(), postgres.getPort(), POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD );

            try {
                LogicalTable source = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceTable, 30 );
                LogicalTable materialization = TestHelper.createIndependentSourceMaterialization( source, materializedTable, storeAdapter );

                assertEquals( materializedTable, materialization.name );
                assertEquals( List.of( "id", "name" ), TestHelper.getCatalogColumnNames( materialization.id ) );
                assertEquals( 2, TestHelper.countRows( materializedTable ) );
            } finally {
                dropRelationalTableIfPresent( materializedTable );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
            }
        }
    }


    @Test
    void relationalSynchronizedMaterializationRefreshCopiesChangedSourceRows() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String sourceTable = "mat_rel_source_" + SUFFIX;
        String materializedTable = "mat_rel_target_" + SUFFIX;
        String sourceAdapter = "pg_mat_source_" + SUFFIX;
        String storeAdapter = "pg_mat_store_" + SUFFIX;

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + sourceTable + " (id INTEGER PRIMARY KEY, name VARCHAR(100))" );
            postgres.execute( "INSERT INTO public." + sourceTable + " (id, name) VALUES (1, 'Alice'), (2, 'Bob')" );
            TestHelper.addPostgresSource( sourceAdapter, postgres.getHost(), postgres.getPort(), POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, "public." + sourceTable );
            TestHelper.addPostgresStore( storeAdapter, postgres.getHost(), postgres.getPort(), POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD );

            try {
                LogicalTable source = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceTable, 30 );
                LogicalTable materialization = TestHelper.createSynchronizedSourceMaterialization( source, materializedTable, storeAdapter );
                assertEquals( 2, TestHelper.countRows( materializedTable ) );

                postgres.execute( "INSERT INTO public." + sourceTable + " (id, name) VALUES (3, 'Carol')" );
                TestHelper.refreshSynchronizedMaterializationData( materialization.id );

                assertEquals( 3, TestHelper.countRows( materializedTable ) );
            } finally {
                dropRelationalTableIfPresent( materializedTable );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
            }
        }
    }


    @Test
    void synchronizedMaterializationAppliesAddedSourceColumn() throws Exception {
        runRelationalSchemaRefreshCase(
                "add_col",
                "id INTEGER PRIMARY KEY, name VARCHAR(100)",
                "id, name",
                "1, 'Alice'",
                postgres -> postgres.execute( "ALTER TABLE public.%s ADD COLUMN age INTEGER".formatted( postgres.tableName() ) ),
                materialization -> {
                    List<String> changes = TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Added columns:" ) ) );
                    assertEquals( List.of( "id", "name", "age" ), TestHelper.getCatalogColumnNames( materialization.id ) );
                } );
    }


    @Test
    void synchronizedMaterializationAppliesRemovedSourceColumn() throws Exception {
        runRelationalSchemaRefreshCase(
                "drop_col",
                "id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER",
                "id, name, age",
                "1, 'Alice', 30",
                postgres -> postgres.execute( "ALTER TABLE public.%s DROP COLUMN age".formatted( postgres.tableName() ) ),
                materialization -> {
                    List<String> changes = TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Removed columns:" ) ) );
                    assertEquals( List.of( "id", "name" ), TestHelper.getCatalogColumnNames( materialization.id ) );
                } );
    }


    @Test
    void synchronizedMaterializationAppliesRenamedSourceColumn() throws Exception {
        runRelationalSchemaRefreshCase(
                "rename_col",
                "id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER",
                "id, name, age",
                "1, 'Alice', 30",
                postgres -> postgres.execute( "ALTER TABLE public.%s RENAME COLUMN name TO full_name".formatted( postgres.tableName() ) ),
                materialization -> {
                    TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertEquals( List.of( "id", "full_name", "age" ), TestHelper.getCatalogColumnNames( materialization.id ) );
                } );
    }


    @Test
    void synchronizedMaterializationAppliesSourceColumnTypeChange() throws Exception {
        runRelationalSchemaRefreshCase(
                "type_col",
                "id INTEGER PRIMARY KEY, age INTEGER",
                "id, age",
                "1, 30",
                postgres -> postgres.execute( "ALTER TABLE public.%s ALTER COLUMN age TYPE BIGINT".formatted( postgres.tableName() ) ),
                materialization -> {
                    List<String> changes = TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Changed column types:" ) ) );
                    assertEquals( PolyType.BIGINT, Catalog.snapshot().rel().getColumn( materialization.id, "age" ).orElseThrow().type );
                } );
    }


    @Test
    void synchronizedMaterializationAppliesSourceColumnNullabilityChange() throws Exception {
        runRelationalSchemaRefreshCase(
                "null_col",
                "id INTEGER PRIMARY KEY, name VARCHAR(100), city VARCHAR(100) NOT NULL",
                "id, name, city",
                "1, 'Alice', 'Basel'",
                postgres -> {
                    postgres.execute( "ALTER TABLE public.%s ALTER COLUMN name SET NOT NULL".formatted( postgres.tableName() ) );
                    postgres.execute( "ALTER TABLE public.%s ALTER COLUMN city DROP NOT NULL".formatted( postgres.tableName() ) );
                },
                materialization -> {
                    List<String> changes = TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Changed column nullability:" ) ) );
                    assertFalse( Catalog.snapshot().rel().getColumn( materialization.id, "name" ).orElseThrow().nullable );
                    assertTrue( Catalog.snapshot().rel().getColumn( materialization.id, "city" ).orElseThrow().nullable );
                } );
    }


    @Test
    void synchronizedMaterializationAppliesSourcePrimaryKeyChange() throws Exception {
        runRelationalSchemaRefreshCase(
                "pk_col",
                "id INTEGER, name VARCHAR(100) NOT NULL, city VARCHAR(100), CONSTRAINT refresh_pk PRIMARY KEY (id)",
                "id, name, city",
                "1, 'Alice', 'Basel'",
                postgres -> {
                    postgres.execute( "ALTER TABLE public.%s DROP CONSTRAINT refresh_pk".formatted( postgres.tableName() ) );
                    postgres.execute( "ALTER TABLE public.%s ADD CONSTRAINT refresh_pk PRIMARY KEY (name)".formatted( postgres.tableName() ) );
                },
                materialization -> {
                    List<String> changes = TestHelper.applySynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Changed primary key:" ) ) );
                    assertEquals( List.of( "id", "name", "city" ), TestHelper.getCatalogColumnNames( materialization.id ) );
                    assertEquals( List.of( "name" ), getPrimaryKeyColumnNames( materialization.id ) );
                } );
    }


    @Test
    void synchronizedMaterializationAppliesForeignKeyWhenReferencedTableIsMaterialized() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String parentSourceTable = "mat_fk_parent_source_" + SUFFIX;
        String childSourceTable = "mat_fk_child_source_" + SUFFIX;
        String parentMaterializedTable = "mat_fk_parent_target_" + SUFFIX;
        String childMaterializedTable = "mat_fk_child_target_" + SUFFIX;
        String sourceAdapter = "pg_mat_fk_source_" + SUFFIX;
        String storeAdapter = "pg_mat_fk_store_" + SUFFIX;
        String database = POSTGRES_DATABASE + "_fk";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( database, POSTGRES_USERNAME, POSTGRES_PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + parentSourceTable + " (id INTEGER PRIMARY KEY, name VARCHAR(100))" );
            postgres.execute( "CREATE TABLE public." + childSourceTable + " (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT mat_fk_child_parent FOREIGN KEY (parent_id) REFERENCES public." + parentSourceTable + " (id))" );
            postgres.execute( "INSERT INTO public." + parentSourceTable + " (id, name) VALUES (1, 'Alice')" );
            postgres.execute( "INSERT INTO public." + childSourceTable + " (id, parent_id) VALUES (1, 1)" );
            TestHelper.addPostgresSource( sourceAdapter, postgres.getHost(), postgres.getPort(), database, POSTGRES_USERNAME, POSTGRES_PASSWORD );
            TestHelper.addPostgresStore( storeAdapter, postgres.getHost(), postgres.getPort(), database, POSTGRES_USERNAME, POSTGRES_PASSWORD );

            try {
                LogicalTable parentSource = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, parentSourceTable, 30 );
                LogicalTable childSource = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, childSourceTable, 30 );
                LogicalTable parentMaterialization = TestHelper.createSynchronizedSourceMaterialization( parentSource, parentMaterializedTable, storeAdapter );
                LogicalTable childMaterialization = TestHelper.createSynchronizedSourceMaterialization( childSource, childMaterializedTable, storeAdapter );

                assertTrue( getForeignKeyNames( childMaterialization.id ).isEmpty() );
                List<String> changes = TestHelper.applySynchronizedMaterializationSchemaRefresh( childMaterialization.id );

                assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Added foreign keys:" ) ) );
                assertEquals( List.of( "mat_fk_child_parent" ), getForeignKeyNames( childMaterialization.id ) );
                assertEquals( List.of( "id" ), getPrimaryKeyColumnNames( parentMaterialization.id ) );
            } finally {
                dropRelationalTableIfPresent( childMaterializedTable );
                dropRelationalTableIfPresent( parentMaterializedTable );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
            }
        }
    }


    @Test
    void synchronizedMaterializationPreviewDoesNotApplyRejectedSourceChanges() throws Exception {
        runRelationalSchemaRefreshCase(
                "reject_col",
                "id INTEGER PRIMARY KEY, name VARCHAR(100)",
                "id, name",
                "1, 'Alice'",
                postgres -> postgres.execute( "ALTER TABLE public.%s ADD COLUMN age INTEGER".formatted( postgres.tableName() ) ),
                materialization -> {
                    List<String> changes = TestHelper.previewSynchronizedMaterializationSchemaRefresh( materialization.id );
                    assertTrue( changes.stream().anyMatch( c -> c.startsWith( "Added columns:" ) ) );
                    assertEquals( List.of( "id", "name" ), TestHelper.getCatalogColumnNames( materialization.id ) );
                    assertTrue( Catalog.snapshot().rel().getColumn( materialization.id, "age" ).isEmpty() );
                } );
    }


    @Test
    void documentSynchronizedMaterializationRefreshCopiesChangedSourceDocuments() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for MongoDB integration tests" );
        TestHelper.getInstance();

        String database = "mongo_mat_refresh_" + SUFFIX;
        String sourceCollection = "mat_doc_source_" + SUFFIX;
        String materializedCollection = "mat_doc_target_" + SUFFIX;
        String sourceAdapter = "mongo_mat_source_" + SUFFIX;
        String storeAdapter = "mongo_mat_store_" + SUFFIX;

        try ( TestHelper.DockerMongo mongo = TestHelper.startMongoDocker( database ) ) {
            mongo.execute( "db." + sourceCollection + ".insertMany([{ name: 'Alice' }, { name: 'Bob' }])" );
            TestHelper.addMongoSource( sourceAdapter, mongo.getHost(), mongo.getPort(), database );
            TestHelper.addMongoStore( storeAdapter );

            try {
                long namespaceId = TestHelper.awaitDocumentNamespaceId( sourceAdapter, 30 );
                LogicalCollection source = TestHelper.awaitLogicalCollection( namespaceId, sourceCollection, 30 );
                LogicalCollection materialization = TestHelper.createSynchronizedSourceCollectionMaterialization( source, materializedCollection, storeAdapter );
                String namespaceName = Catalog.snapshot().getNamespace( namespaceId ).orElseThrow().name;
                assertEquals( 2, TestHelper.countDocuments( namespaceName, materializedCollection ) );

                mongo.execute( "db." + sourceCollection + ".insertOne({ name: 'Carol' })" );
                TestHelper.refreshSynchronizedCollectionMaterializationData( materialization.id );

                assertEquals( 3, TestHelper.countDocuments( namespaceName, materializedCollection ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
            }
        }
    }


    private static void runRelationalSchemaRefreshCase(
            String scenario,
            String tableDefinition,
            String insertColumns,
            String insertValues,
            ThrowingConsumer<SourceContext> mutateSource,
            ThrowingConsumer<LogicalTable> assertMaterialization ) throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String sourceTable = "mat_" + scenario + "_source_" + SUFFIX;
        String materializedTable = "mat_" + scenario + "_target_" + SUFFIX;
        String sourceAdapter = "pg_mat_" + scenario + "_source_" + SUFFIX;
        String storeAdapter = "pg_mat_" + scenario + "_store_" + SUFFIX;

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( POSTGRES_DATABASE + "_" + scenario, POSTGRES_USERNAME, POSTGRES_PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + sourceTable + " (" + tableDefinition + ")" );
            postgres.execute( "INSERT INTO public." + sourceTable + " (" + insertColumns + ") VALUES (" + insertValues + ")" );
            TestHelper.addPostgresSource( sourceAdapter, postgres.getHost(), postgres.getPort(), POSTGRES_DATABASE + "_" + scenario, POSTGRES_USERNAME, POSTGRES_PASSWORD, "public." + sourceTable );
            TestHelper.addPostgresStore( storeAdapter, postgres.getHost(), postgres.getPort(), POSTGRES_DATABASE + "_" + scenario, POSTGRES_USERNAME, POSTGRES_PASSWORD );

            try {
                LogicalTable source = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceTable, 30 );
                LogicalTable materialization = TestHelper.createSynchronizedSourceMaterialization( source, materializedTable, storeAdapter );
                assertEquals( TestHelper.getCatalogColumnNames( source.id ), TestHelper.getCatalogColumnNames( materialization.id ) );

                mutateSource.accept( new SourceContext( postgres, sourceTable ) );
                assertMaterialization.accept( materialization );
            } finally {
                dropRelationalTableIfPresent( materializedTable );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + sourceAdapter + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + storeAdapter + "\"" );
            }
        }
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


    private static List<String> getForeignKeyNames( long entityId ) {
        return Catalog.snapshot().rel().getForeignKeys( entityId ).stream()
                .map( LogicalForeignKey::getName )
                .toList();
    }


    private static void dropRelationalTableIfPresent( String tableName ) throws Exception {
        if ( Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, tableName ).isPresent() ) {
            TestHelper.executeSQL( "DROP TABLE \"" + Catalog.DEFAULT_NAMESPACE_NAME + "\".\"" + tableName + "\"" );
        }
    }


    private record SourceContext( TestHelper.DockerPostgres postgres, String tableName ) {

        void execute( String statement ) throws Exception {
            postgres.execute( statement );
        }

    }


    @FunctionalInterface
    private interface ThrowingConsumer<T> {

        void accept( T value ) throws Exception;

    }

}
