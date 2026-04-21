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


    private static List<String> getPrimaryKeyColumnNames( long entityId ) {
        Long primaryKey = Catalog.snapshot().rel().getTable( entityId ).orElseThrow().primaryKey;
        if ( primaryKey == null ) {
            return List.of();
        }
        return Catalog.snapshot().rel().getPrimaryKey( primaryKey ).orElseThrow().fieldIds.stream()
                .map( id -> Catalog.snapshot().rel().getColumn( id ).orElseThrow().name )
                .toList();
    }
}
