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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.type.VectorType;

/**
 *
 * <p>Reuses the existing PostgreSQL Store container,
 * prepopulates it with {@code boolean[]} and {@code bit(n)} columns, attaches it
 * as a Polypheny source, then asserts that the catalog reflects the correct internal types:
 * <ul>
 *   <li>{@code boolean[]} -> plain {@code ARRAY<BOOLEAN>}, cardinality {@code null} -> <b>not</b> VectorType</li>
 *   <li>{@code bit(5)} -> {@code ARRAY<BOOLEAN>} with cardinality 5 -> is VectorType&lt;BIT&gt;(5)</li>
 * </ul>
 */
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "postgresql")
public class PostgresqlSourceDiscoveryTest {

    private static final String SOURCE_ADAPTER = "pg_discovery_source";
    private static final String TABLE_NAME     = "public.discovery_test";
    private static final String RAW_TABLE_NAME = "discovery_test";

    private static boolean setupSucceeded = false;


    @BeforeAll
    static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();

        Optional<LogicalAdapter> maybeStore = Catalog.getInstance().getSnapshot().getAdapters().stream()
                .filter( ad -> ad.type == AdapterType.STORE && ad.adapterName.equalsIgnoreCase( "PostgreSQL" ) )
                .findFirst();

        assertTrue( maybeStore.isPresent(), "PostgreSQL Store not found in Catalog - skipping test" );

        LogicalAdapter pgStore = maybeStore.get();
        Map<String, String> settingsMap = pgStore.settings;
        String host;
        int port;
        if ( pgStore.mode == DeployMode.DOCKER ) {
            String deploymentId = settingsMap.get( "deploymentId" );
            DockerContainer container = DockerContainer.getContainerByUUID( deploymentId )
                    .orElseThrow( () -> new RuntimeException( "Could not find docker container for PostgreSQL store" ) );
            DockerContainer.HostAndPort hp = container.connectToContainer( 5432 );
            host = hp.host();
            port = hp.port();
        } else {
            host = settingsMap.get( "host" );
            port = Integer.parseInt( settingsMap.get( "port" ) );
        }

        String database = settingsMap.getOrDefault( "database", "postgres" );
        String username = settingsMap.getOrDefault( "username", "postgres" );
        String password = settingsMap.getOrDefault( "password", "polypheny" );

        String jdbcUrl = String.format( "jdbc:postgresql://%s:%d/%s", host, port, database );
        try ( Connection conn = DriverManager.getConnection( jdbcUrl, username, password );
              Statement st = conn.createStatement() ) {
            st.executeUpdate( "DROP TABLE IF EXISTS " + TABLE_NAME );
            st.executeUpdate(
                    "CREATE TABLE " + TABLE_NAME + " (" +
                    "  id         SERIAL PRIMARY KEY," +
                    "  bool_array BOOLEAN[]," +
                    "  bit_vector BIT(5)" +
                    ")" );
        }


        String settings = String.format(
                "'{ \"mode\": \"REMOTE\", \"host\": \"%s\", \"port\": \"%d\", \"database\": \"%s\", \"username\": \"%s\", \"password\": \"%s\", \"tables\": \"%s\", \"maxConnections\": \"25\", \"transactionIsolation\": \"SERIALIZABLE\" }'",
                host, port, database, username, password, TABLE_NAME );
        try ( JdbcConnection jc = new JdbcConnection( true );
              Statement st = jc.getConnection().createStatement() ) {
            st.executeUpdate( "ALTER ADAPTERS ADD \"" + SOURCE_ADAPTER + "\"" +
                    " USING 'PostgreSQL' AS 'Source' WITH " + settings );
        }
        setupSucceeded = true;
    }


    @AfterAll
    static void stop() {
        if ( setupSucceeded ) {
            try ( JdbcConnection jc = new JdbcConnection( true );
                  Statement st = jc.getConnection().createStatement() ) {
                st.executeUpdate( "ALTER ADAPTERS DROP \"" + SOURCE_ADAPTER + "\"" );
            } catch ( Exception e ) {
                log.warn( "Could not drop source adapter during teardown", e );
            }
        }
    }


    private List<LogicalColumn> columns() {
        LogicalTable table = Catalog.getInstance().getSnapshot().rel()
                .getTable( "public", RAW_TABLE_NAME ).orElseThrow();
        return Catalog.getInstance().getSnapshot().rel().getColumns( table.id );
    }


    @Test
    void boolArrayDiscoveredAsPlainArrayNotVector() {
        LogicalColumn col = columns().stream()
                .filter( c -> c.name.equals( "bool_array" ) )
                .findFirst().orElseThrow();

        assertEquals( PolyType.BOOLEAN, col.type );
        assertEquals( PolyType.ARRAY, col.collectionsType );
        // createArrayType must NOT promote this to VectorType iff cardinality is null
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

        assertInstanceOf( VectorType.class, col.getAlgDataType( AlgDataTypeFactory.DEFAULT ), "bit(5) must be promoted to VectorType<BIT>" );
        VectorType vt = (VectorType) col.getAlgDataType( AlgDataTypeFactory.DEFAULT );
        assertEquals( 5, vt.getVectorDimension() );
        assertEquals( VectorType.ElementType.BIT, vt.getVectorElementType() );
    }

}
