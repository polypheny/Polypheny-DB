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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.webui.models.results.RelationalResult;


@Tag("adapter")
@SuppressWarnings("SqlDialectInspection")
class SourceSchemaRefreshTest {

    private static final String SUFFIX = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
    private static final String SOURCE_TABLE = "refresh_source_" + SUFFIX;
    private static final String ADAPTER_NAME = "pg_refresh_src_" + SUFFIX;
    private static final String DATABASE = "polypheny_refresh";
    private static final String USERNAME = "polypheny";
    private static final String PASSWORD = "polypheny";


    @Test
    void refreshRequestUpdatesPolyphenyColumnsAfterExternalColumnsWereAdded() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (id, name) VALUES (1, 'Alice')" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( List.of( "id", "name" ), TestHelper.getCatalogColumnNames( entityId ) );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ADD COLUMN age INTEGER" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ADD COLUMN city VARCHAR(255)" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "name", "age", "city" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( List.of( "id", "name", "age", "city" ), TestHelper.getCatalogColumnNames( entityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestUpdatesPolyphenyColumnsAfterExternalColumnWasDropped() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (id INTEGER PRIMARY KEY, name VARCHAR(255), age INTEGER)" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (id, name, age) VALUES (1, 'Alice', 30)" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( List.of( "id", "name", "age" ), TestHelper.getCatalogColumnNames( entityId ) );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " DROP COLUMN age" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "name" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( List.of( "id", "name" ), TestHelper.getCatalogColumnNames( entityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestKeepsRenamedExternalColumnAtItsOriginalPosition() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (id INTEGER PRIMARY KEY, name VARCHAR(255), age INTEGER)" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (id, name, age) VALUES (1, 'Alice', 30)" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( List.of( "id", "name", "age" ), TestHelper.getCatalogColumnNames( entityId ) );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " RENAME COLUMN name TO full_name" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "full_name", "age" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( List.of( "id", "full_name", "age" ), TestHelper.getCatalogColumnNames( entityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestUpdatesPolyphenyColumnTypeAfterExternalColumnTypeChanged() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (id INTEGER PRIMARY KEY, age INTEGER)" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (id, age) VALUES (1, 30)" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( PolyType.INTEGER, Catalog.snapshot().rel().getColumn( entityId, "age" ).orElseThrow().type );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ALTER COLUMN age TYPE BIGINT" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "age" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( PolyType.BIGINT.getName(), refreshed.getHeader()[1].dataType );
                assertEquals( PolyType.BIGINT, Catalog.snapshot().rel().getColumn( entityId, "age" ).orElseThrow().type );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestUpdatesPolyphenyColumnNullabilityAfterExternalNullabilityChanged() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (id INTEGER PRIMARY KEY, name VARCHAR(255), city VARCHAR(255) NOT NULL)" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (id, name, city) VALUES (1, 'Alice', 'Basel')" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertTrue( Catalog.snapshot().rel().getColumn( entityId, "name" ).orElseThrow().nullable );
                assertFalse( Catalog.snapshot().rel().getColumn( entityId, "city" ).orElseThrow().nullable );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ALTER COLUMN name SET NOT NULL" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ALTER COLUMN city DROP NOT NULL" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "name", "city" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertFalse( Catalog.snapshot().rel().getColumn( entityId, "name" ).orElseThrow().nullable );
                assertTrue( Catalog.snapshot().rel().getColumn( entityId, "city" ).orElseThrow().nullable );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestUpdatesPolyphenyColumnOrderAfterExternalColumnsWereReordered() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for MySQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerMysql mysql = TestHelper.startMysqlDocker( DATABASE, USERNAME, PASSWORD ) ) {
            mysql.execute( "CREATE TABLE " + SOURCE_TABLE + " (id INTEGER PRIMARY KEY, name VARCHAR(255), city VARCHAR(255))" );
            mysql.execute( "INSERT INTO " + SOURCE_TABLE + " (id, name, city) VALUES " +
                    "(1, 'Alice', 'Basel'), " +
                    "(2, 'Bob', 'Zurich'), " +
                    "(3, 'Carol', 'Bern')" );

            TestHelper.addMysqlSource(
                    ADAPTER_NAME,
                    mysql.getHost(),
                    mysql.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( List.of( "id", "name", "city" ), TestHelper.getCatalogColumnNames( entityId ) );

                mysql.execute( "ALTER TABLE " + SOURCE_TABLE + " MODIFY COLUMN city VARCHAR(255) FIRST" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "city", "id", "name" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( 3, refreshed.getData().length );
                assertEquals( List.of( "city", "id", "name" ), TestHelper.getCatalogColumnNames( entityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestUpdatesPrimaryKeyAfterExternalPrimaryKeyChanged() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (id INTEGER, name VARCHAR(255) NOT NULL, city VARCHAR(255), CONSTRAINT refresh_pk PRIMARY KEY (id))" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (id, name, city) VALUES " +
                    "(1, 'Alice', 'Basel'), " +
                    "(2, 'Bob', 'Zurich'), " +
                    "(3, 'Carol', 'Bern')" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( List.of( "id" ), getPrimaryKeyColumnNames( entityId ) );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " DROP CONSTRAINT refresh_pk" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ADD CONSTRAINT refresh_pk PRIMARY KEY (name)" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " DROP COLUMN id" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "name", "city" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( List.of( "name", "city" ), TestHelper.getCatalogColumnNames( entityId ) );
                assertEquals( List.of( "name" ), getPrimaryKeyColumnNames( entityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestUpdatesForeignKeysAfterExternalForeignKeysChanged() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String parentTable = SOURCE_TABLE + "_parent";
        String childTable = SOURCE_TABLE + "_child";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + parentTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgres.execute( "CREATE TABLE public." + childTable + " (id INTEGER PRIMARY KEY, parent_id INTEGER)" );
            postgres.execute( "INSERT INTO public." + parentTable + " (id, name) VALUES (1, 'Alice')" );
            postgres.execute( "INSERT INTO public." + childTable + " (id, parent_id) VALUES (1, 1)" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + parentTable + ",public." + childTable );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, parentTable, 30 );
                long childEntityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, childTable, 30 ).id;
                assertEquals( List.of(), getForeignKeyNames( childEntityId ) );

                postgres.execute( "ALTER TABLE public." + childTable + " ADD CONSTRAINT refresh_fk_parent FOREIGN KEY (parent_id) REFERENCES public." + parentTable + " (id)" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( childEntityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "refresh_fk_parent" ), getForeignKeyNames( childEntityId ) );

                postgres.execute( "ALTER TABLE public." + childTable + " DROP CONSTRAINT refresh_fk_parent" );

                refreshed = TestHelper.sendRefreshRequest( childEntityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of(), getForeignKeyNames( childEntityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestTracksForeignKeyAcrossColumnAdditionAndRemoval()throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String professorTable = SOURCE_TABLE + "_professor";
        String enrollmentTable = SOURCE_TABLE + "_enrollment";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + professorTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgres.execute( "CREATE TABLE public." + enrollmentTable + " (id INTEGER PRIMARY KEY, course_id INTEGER, semester VARCHAR(16), attempt INTEGER, note VARCHAR(255))" );
            postgres.execute( "INSERT INTO public." + professorTable + " (id, name) VALUES (1, 'Ada')" );
            postgres.execute( "INSERT INTO public." + enrollmentTable + " (id, course_id, semester, attempt, note) VALUES (1, 10, 'FS26', 1, 'initial')" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + professorTable + ",public." + enrollmentTable );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, professorTable, 30 );
                long enrollmentEntityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, enrollmentTable, 30 ).id;
                assertEquals( List.of( "id", "course_id", "semester", "attempt", "note" ), TestHelper.getCatalogColumnNames( enrollmentEntityId ) );
                assertEquals( List.of(), getForeignKeyNames( enrollmentEntityId ) );

                postgres.execute( "ALTER TABLE public." + enrollmentTable + " ADD COLUMN professor_id INTEGER" );
                postgres.execute( "ALTER TABLE public." + enrollmentTable + " ADD CONSTRAINT refresh_fk_professor FOREIGN KEY (professor_id) REFERENCES public." + professorTable + " (id)" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( enrollmentEntityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "course_id", "semester", "attempt", "note", "professor_id" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( List.of( "id", "course_id", "semester", "attempt", "note", "professor_id" ), TestHelper.getCatalogColumnNames( enrollmentEntityId ) );
                assertEquals( List.of( "refresh_fk_professor" ), getForeignKeyNames( enrollmentEntityId ) );

                postgres.execute( "ALTER TABLE public." + enrollmentTable + " DROP CONSTRAINT refresh_fk_professor" );
                postgres.execute( "ALTER TABLE public." + enrollmentTable + " DROP COLUMN professor_id" );

                refreshed = TestHelper.sendRefreshRequest( enrollmentEntityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "id", "course_id", "semester", "attempt", "note" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( List.of( "id", "course_id", "semester", "attempt", "note" ), TestHelper.getCatalogColumnNames( enrollmentEntityId ) );
                assertEquals( List.of(), getForeignKeyNames( enrollmentEntityId ) );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void refreshRequestAppliesMixedExternalSchemaChanges() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + SOURCE_TABLE + " (code VARCHAR(16) PRIMARY KEY, name VARCHAR(255), age INTEGER, city VARCHAR(255))" );
            postgres.execute( "INSERT INTO public." + SOURCE_TABLE + " (code, name, age, city) VALUES " +
                    "('1', 'Alice', 30, 'Basel'), " +
                    "('2', 'Bob', 31, 'Zurich'), " +
                    "('3', 'Carol', 32, 'Bern'), " +
                    "('4', 'Dave', 33, 'Geneva'), " +
                    "('5', 'Eve', 34, 'Lausanne'), " +
                    "('6', 'Frank', 35, 'Lugano'), " +
                    "('7', 'Grace', 36, 'St. Gallen'), " +
                    "('8', 'Heidi', 37, 'Lucerne'), " +
                    "('9', 'Ivan', 38, 'Winterthur'), " +
                    "('10', 'Judy', 39, 'Biel')" );

            TestHelper.addPostgresSource(
                    ADAPTER_NAME,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD,
                    "public." + SOURCE_TABLE );

            try {
                long entityId = TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, SOURCE_TABLE, 30 ).id;
                assertEquals( List.of( "code", "name", "age", "city" ), TestHelper.getCatalogColumnNames( entityId ) );
                assertEquals( PolyType.VARCHAR, Catalog.snapshot().rel().getColumn( entityId, "code" ).orElseThrow().type );
                assertTrue( Catalog.snapshot().rel().getColumn( entityId, "city" ).orElseThrow().nullable );

                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " DROP COLUMN name" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " DROP COLUMN age" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ADD COLUMN country VARCHAR(255) DEFAULT 'CH' NOT NULL" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ALTER COLUMN code TYPE INTEGER USING code::integer" );
                postgres.execute( "ALTER TABLE public." + SOURCE_TABLE + " ALTER COLUMN city SET NOT NULL" );

                RelationalResult refreshed = TestHelper.sendRefreshRequest( entityId );
                assertNotNull( refreshed );
                assertTrue( refreshed.getError() == null || refreshed.getError().isBlank(), "Refresh returned error: " + refreshed.getError() );
                assertEquals( List.of( "code", "city", "country" ), Arrays.stream( refreshed.getHeader() ).map( h -> h.getName() ).toList() );
                assertEquals( PolyType.INTEGER.getName(), refreshed.getHeader()[0].dataType );
                assertEquals( 10, refreshed.getData().length );
                assertEquals( List.of( "code", "city", "country" ), TestHelper.getCatalogColumnNames( entityId ) );
                assertEquals( PolyType.INTEGER, Catalog.snapshot().rel().getColumn( entityId, "code" ).orElseThrow().type );
                assertTrue( !Catalog.snapshot().rel().getColumn( entityId, "city" ).orElseThrow().nullable );
                assertTrue( !Catalog.snapshot().rel().getColumn( entityId, "country" ).orElseThrow().nullable );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void selectedSourceRefreshAddsNewlyDetectedTable() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String adapterName = ADAPTER_NAME + "_table_add";
        String existingTable = SOURCE_TABLE + "_existing_add";
        String addedTable = SOURCE_TABLE + "_added";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + existingTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgres.execute( "INSERT INTO public." + existingTable + " (id, name) VALUES (1, 'Alice')" );

            TestHelper.addPostgresSource(
                    adapterName,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, existingTable, 30 );
                assertFalse( Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, addedTable ).isPresent() );

                postgres.execute( "CREATE TABLE public." + addedTable + " (id INTEGER PRIMARY KEY, city VARCHAR(255))" );
                postgres.execute( "INSERT INTO public." + addedTable + " (id, city) VALUES (1, 'Basel')" );

                long sourceId = TestHelper.awaitSourceAdapterId( adapterName, 30 );
                List<String> refreshedTables = TestHelper.refreshSelectedSources( List.of( sourceId ) );

                assertTrue( refreshedTables.contains( existingTable ) );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, addedTable, 30 );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void selectedSourceRefreshDropsRemovedTable() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String adapterName = ADAPTER_NAME + "_table_drop";
        String keptTable = SOURCE_TABLE + "_kept_drop";
        String removedTable = SOURCE_TABLE + "_removed_drop";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + keptTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgres.execute( "CREATE TABLE public." + removedTable + " (id INTEGER PRIMARY KEY, city VARCHAR(255))" );

            TestHelper.addPostgresSource(
                    adapterName,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, keptTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, removedTable, 30 );

                postgres.execute( "DROP TABLE public." + removedTable );

                long sourceId = TestHelper.awaitSourceAdapterId( adapterName, 30 );
                List<String> refreshedTables = TestHelper.refreshSelectedSources( List.of( sourceId ) );

                assertTrue( refreshedTables.contains( keptTable ) );
                TestHelper.awaitLogicalTableAbsent( Catalog.defaultNamespaceId, removedTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, keptTable, 30 );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void selectedSourceRefreshReplacesRenamedTable() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String adapterName = ADAPTER_NAME + "_table_rename";
        String oldTable = SOURCE_TABLE + "_old_name";
        String newTable = SOURCE_TABLE + "_new_name";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + oldTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );

            TestHelper.addPostgresSource(
                    adapterName,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, oldTable, 30 );
                assertFalse( Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, newTable ).isPresent() );

                postgres.execute( "ALTER TABLE public." + oldTable + " RENAME TO " + newTable );

                long sourceId = TestHelper.awaitSourceAdapterId( adapterName, 30 );
                List<String> refreshedTables = TestHelper.refreshSelectedSources( List.of( sourceId ) );

                assertNotNull( refreshedTables );
                TestHelper.awaitLogicalTableAbsent( Catalog.defaultNamespaceId, oldTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, newTable, 30 );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void selectedSourceRefreshAppliesCombinedTableDiscoveryChanges() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String adapterName = ADAPTER_NAME + "_table_combined";
        String keptTable = SOURCE_TABLE + "_kept_combined";
        String removedTable = SOURCE_TABLE + "_removed_combined";
        String oldRenamedTable = SOURCE_TABLE + "_old_combined";
        String newRenamedTable = SOURCE_TABLE + "_new_combined";
        String addedTable = SOURCE_TABLE + "_added_combined";

        try ( TestHelper.DockerPostgres postgres = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD ) ) {
            postgres.execute( "CREATE TABLE public." + keptTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgres.execute( "CREATE TABLE public." + removedTable + " (id INTEGER PRIMARY KEY, city VARCHAR(255))" );
            postgres.execute( "CREATE TABLE public." + oldRenamedTable + " (id INTEGER PRIMARY KEY, age INTEGER)" );

            TestHelper.addPostgresSource(
                    adapterName,
                    postgres.getHost(),
                    postgres.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, keptTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, removedTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, oldRenamedTable, 30 );

                postgres.execute( "DROP TABLE public." + removedTable );
                postgres.execute( "ALTER TABLE public." + oldRenamedTable + " RENAME TO " + newRenamedTable );
                postgres.execute( "CREATE TABLE public." + addedTable + " (id INTEGER PRIMARY KEY, country VARCHAR(255))" );

                long sourceId = TestHelper.awaitSourceAdapterId( adapterName, 30 );
                List<String> refreshedTables = TestHelper.refreshSelectedSources( List.of( sourceId ) );

                assertTrue( refreshedTables.contains( keptTable ) );

                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, keptTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, newRenamedTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, addedTable, 30 );
                TestHelper.awaitLogicalTableAbsent( Catalog.defaultNamespaceId, removedTable, 30 );
                TestHelper.awaitLogicalTableAbsent( Catalog.defaultNamespaceId, oldRenamedTable, 30 );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void selectedSourceRefreshOnlyAppliesChangesForRequestedSource() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for PostgreSQL integration tests" );
        TestHelper.getInstance();

        String adapterOne = ADAPTER_NAME + "_source_one";
        String adapterTwo = ADAPTER_NAME + "_source_two";
        String sourceOneTable = SOURCE_TABLE + "_source_one";
        String sourceTwoTable = SOURCE_TABLE + "_source_two";
        String sourceOneAddedTable = SOURCE_TABLE + "_source_one_added";
        String sourceTwoAddedTable = SOURCE_TABLE + "_source_two_added";

        try ( TestHelper.DockerPostgres postgresOne = TestHelper.startPostgresDocker( DATABASE, USERNAME, PASSWORD );
              TestHelper.DockerPostgres postgresTwo = TestHelper.startPostgresDocker( DATABASE + "_two", USERNAME, PASSWORD ) ) {
            postgresOne.execute( "CREATE TABLE public." + sourceOneTable + " (id INTEGER PRIMARY KEY, name VARCHAR(255))" );
            postgresTwo.execute( "CREATE TABLE public." + sourceTwoTable + " (id INTEGER PRIMARY KEY, city VARCHAR(255))" );

            TestHelper.addPostgresSource(
                    adapterOne,
                    postgresOne.getHost(),
                    postgresOne.getPort(),
                    DATABASE,
                    USERNAME,
                    PASSWORD );

            TestHelper.addPostgresSource(
                    adapterTwo,
                    postgresTwo.getHost(),
                    postgresTwo.getPort(),
                    DATABASE + "_two",
                    USERNAME,
                    PASSWORD );

            try {
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceOneTable, 30 );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceTwoTable, 30 );

                postgresOne.execute( "CREATE TABLE public." + sourceOneAddedTable + " (id INTEGER PRIMARY KEY, age INTEGER)" );
                postgresTwo.execute( "CREATE TABLE public." + sourceTwoAddedTable + " (id INTEGER PRIMARY KEY, country VARCHAR(255))" );

                long sourceOneId = TestHelper.awaitSourceAdapterId( adapterOne, 30 );
                List<String> refreshedTables = TestHelper.refreshSelectedSources( List.of( sourceOneId ) );

                assertTrue( refreshedTables.contains( sourceOneTable ) );
                TestHelper.awaitLogicalTable( Catalog.defaultNamespaceId, sourceOneAddedTable, 30 );
                assertFalse( Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, sourceTwoAddedTable ).isPresent() );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterOne + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterTwo + "\"" );
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
}
