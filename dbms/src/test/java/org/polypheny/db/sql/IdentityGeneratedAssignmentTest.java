/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.config.RuntimeConfig;

@Tag("adapter")
class IdentityGeneratedAssignmentTest {

    @BeforeAll
    static void start() {
        TestHelper.getInstance();
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void generatedIdsDoNotUseTheSourcePlaceholderPrecision( boolean parameterize ) throws SQLException {
        assumeTrue( List.of( "hsqldb", "postgresql" ).contains( PolyphenyDb.defaultStoreName ) );
        boolean originalParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            Connection connection = jdbc.getConnection();
            sql.executeUpdate( "CREATE TABLE identity_generated_assignment (id BIGSERIAL PRIMARY KEY, note VARCHAR(30))" );
            try {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                sql.executeUpdate( "ALTER TABLE identity_generated_assignment ALTER COLUMN id RESTART WITH 9" );
                String insert = "INSERT INTO identity_generated_assignment (note) VALUES ";
                try ( PreparedStatement plain = connection.prepareStatement( insert + "(?)" );
                        PreparedStatement returning = connection.prepareStatement( insert + "(?)", Statement.RETURN_GENERATED_KEYS ) ) {
                    plain.setString( 1, "before boundary" );
                    assertEquals( 1, plain.executeUpdate() );
                    plain.setString( 1, "after boundary" );
                    assertEquals( 1, plain.executeUpdate() );

                    returning.setString( 1, "prepared keys" );
                    assertEquals( 1, returning.executeUpdate() );
                    assertEquals( List.of( 11L ), ids( returning.getGeneratedKeys() ) );
                }
                assertEquals( 1, sql.executeUpdate( insert + "('raw keys')", Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of( 12L ), ids( sql.getGeneratedKeys() ) );

                String mixed = "INSERT INTO identity_generated_assignment VALUES "
                        + "(DEFAULT, 'generated'), (CAST(42.0 AS DECIMAL(4,1)), 'supplied')";
                assertEquals( 2, sql.executeUpdate( mixed, Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of( 13L ), ids( sql.getGeneratedKeys() ) );
                assertEquals( List.of( 9L, 10L, 11L, 12L, 13L, 42L ),
                        ids( sql.executeQuery( "SELECT id FROM identity_generated_assignment ORDER BY id" ) ) );
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + originalParameterization );
                sql.executeUpdate( "DROP TABLE identity_generated_assignment" );
            }
        }
    }


    private static List<Long> ids( ResultSet rows ) throws SQLException {
        try ( rows ) {
            List<Long> ids = new ArrayList<>();
            while ( rows.next() ) {
                ids.add( rows.getLong( 1 ) );
            }
            return ids;
        }
    }

}
