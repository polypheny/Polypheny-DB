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

package org.polypheny.db.adapter.postgres.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.VectorType;

/**
 * Integration test for PostgreSQL source column-type discovery.
 *
 * <p>Starts a Postgres container, prepopulates it with {@code boolean[]} and
 * {@code bit(n)} columns, attaches it as a Polypheny source, then asserts that the
 * catalog reflects the correct internal types:
 * <ul>
 *   <li>{@code boolean[]} -> plain {@code ARRAY<BOOLEAN>}, cardinality {@code null} -> <b>not</b> VectorType</li>
 *   <li>{@code bit(5)}    -> {@code ARRAY<BOOLEAN>} with cardinality 5 -> <b>is</b> VectorType&lt;BIT&gt;(5)</li>
 * </ul>
 *
 * <p>Requires a local Docker daemon. Skipped automatically when Docker is unavailable.
 */
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class PostgresqlSourceDiscoveryTest {

    private static final String SOURCE_ADAPTER = "pg_discovery_source";
    private static final String TABLE_NAME     = "discovery_test";

    private static DockerContainer container;
    private static boolean setupSucceeded = false;


    @BeforeAll
    static void setup() throws IOException, SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();

        Optional<DockerInstance> maybeDocker = DockerManager.getInstance().getInstanceById( 0 );
        assumeTrue( maybeDocker.isPresent(), "No local Docker instance - skipping source discovery test" );

        container = maybeDocker.get()
                .newBuilder( "polypheny/postgres-pgvector:latest", "pg-discovery-test" )
                .withExposedPort( 5432 )
                .withEnvironmentVariable( "POSTGRES_PASSWORD", "polypheny" )
                .createAndStart();

        boolean started = container.waitTillStarted( () -> {
            HostAndPort hp = container.connectToContainer( 5432 );
            try ( Connection c = DriverManager.getConnection(
                    "jdbc:postgresql://" + hp.host() + ":" + hp.port() + "/postgres",
                    "postgres", "polypheny" ) ) {
                return true;
            } catch ( SQLException e ) {
                return false;
            }
        }, 30_000 );
        assumeTrue( started, "Postgres container did not become ready within 30 s" );

        HostAndPort hp = container.connectToContainer( 5432 );
        try ( Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://" + hp.host() + ":" + hp.port() + "/postgres",
                "postgres", "polypheny" );
              Statement st = conn.createStatement() ) {
            st.executeUpdate(
                    "CREATE TABLE " + TABLE_NAME + " (" +
                    "  id         SERIAL PRIMARY KEY," +
                    "  bool_array BOOLEAN[]," +
                    "  bit_vector BIT(5)" +
                    ")" );
        }

        HostAndPort hp2 = container.connectToContainer( 5432 );
        String settings = String.format(
                "'host'='%s', 'port'='%d', 'database'='postgres'," +
                " 'username'='postgres', 'password'='polypheny', 'tables'='%s'",
                hp2.host(), hp2.port(), TABLE_NAME );
        try ( JdbcConnection jc = new JdbcConnection( true );
              Statement st = jc.getConnection().createStatement() ) {
            st.executeUpdate( "ALTER ADAPTERS ADD \"" + SOURCE_ADAPTER + "\"" +
                    " USING 'PostgreSQL' AS 'Source' WITH (" + settings + ")" );
        }

        setupSucceeded = true;
    }


    @AfterAll
    static void teardown() {
        if ( setupSucceeded ) {
            try ( JdbcConnection jc = new JdbcConnection( true );
                  Statement st = jc.getConnection().createStatement() ) {
                st.executeUpdate( "ALTER ADAPTERS DROP \"" + SOURCE_ADAPTER + "\"" );
            } catch ( Exception e ) {
                log.warn( "Could not drop source adapter during teardown", e );
            }
        }
        if ( container != null ) {
            container.destroy();
        }
    }


    private List<LogicalColumn> columns() {
        LogicalTable table = Catalog.getInstance().getSnapshot().rel()
                .getTable( SOURCE_ADAPTER, TABLE_NAME ).orElseThrow();
        return Catalog.getInstance().getSnapshot().rel().getColumns( table.id );
    }


    @Test
    void boolArrayDiscoveredAsPlainArrayNotVector() {
        LogicalColumn col = columns().stream()
                .filter( c -> c.name.equals( "bool_array" ) )
                .findFirst().orElseThrow();

        assertEquals( PolyType.BOOLEAN, col.type );
        assertEquals( PolyType.ARRAY, col.collectionsType );
        // cardinality must be null - createArrayType must NOT promote this to VectorType
        assertNull( col.cardinality,
                "boolean[] must have null cardinality so it is not mistaken for a bitvector" );
        assertFalse( col.getAlgDataType( AlgDataTypeFactory.DEFAULT ) instanceof VectorType,
                "boolean[] must not be promoted to VectorType" );
    }


    @Test
    void bitColumnDiscoveredAsVectorType() {
        LogicalColumn col = columns().stream()
                .filter( c -> c.name.equals( "bit_vector" ) )
                .findFirst().orElseThrow();

        assertEquals( PolyType.BOOLEAN, col.type );
        assertEquals( PolyType.ARRAY, col.collectionsType );
        assertNotNull( col.cardinality, "bit(5) must have non-null cardinality" );
        assertEquals( 5, col.cardinality );
        assertEquals( 1, col.dimension );

        assertTrue( col.getAlgDataType( AlgDataTypeFactory.DEFAULT ) instanceof VectorType,
                "bit(5) must be promoted to VectorType<BIT>" );
        VectorType vt = (VectorType) col.getAlgDataType( AlgDataTypeFactory.DEFAULT );
        assertEquals( 5, vt.getVectorDimension() );
        assertEquals( VectorType.ElementType.BIT, vt.getVectorElementType() );
    }

}
