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
                    "127.0.0.1",
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
                    "127.0.0.1",
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
                    "127.0.0.1",
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

}
