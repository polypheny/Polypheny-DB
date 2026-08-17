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

package org.polypheny.db.constraints;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
* A column declared as {@code REAL NOT NULL ARRAY(1,n)} or {@code BOOLEAN NOT NULL ARRAY(1,n)}
* is internally mapped to a VectorType. Inserting an array that contains a null element
* must be rejected; inserting a null for the column itself (i.e. a null array) is still
* allowed because the column is not declared NOT NULL at the column level.
*/
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class ArrayNotNullConstraintTest {

    @BeforeAll
    public static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    //--------------------- REAL NOT NULL ARRAY(1,n) ---------------------
    @Test
    void realVectorInsertWithAllNonNullElementsSucceeds() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE vnn_real( id INTEGER NOT NULL, vec REAL NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                try {
                    statement.executeUpdate( "INSERT INTO vnn_real VALUES (1, ARRAY[1.0, 2.0, 3.0])" );
                    connection.commit();
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT vec FROM vnn_real WHERE id = 1" ),
                            ImmutableList.of( new Object[]{ new Object[]{ 1.0f, 2.0f, 3.0f } } ) );
                } finally {
                    statement.executeUpdate( "DROP TABLE vnn_real" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    void realVectorColumnLevelNullIsAllowed() throws SQLException {
        // The column itself has no NOT NULL, so a null array value is permitted.
        // Only element-level nulls are forbidden.
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE vnn_real_colnull( id INTEGER NOT NULL, vec REAL NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                try {
                    statement.executeUpdate( "INSERT INTO vnn_real_colnull VALUES (1, NULL)" );
                    connection.commit();
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT vec FROM vnn_real_colnull WHERE id = 1" ),
                            ImmutableList.of( new Object[]{ null } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE vnn_real_colnull" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    void realVectorLiteralWithNullElementIsRejected() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE vnn_real_rej( id INTEGER NOT NULL, vec REAL NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                try {
                    Assertions.assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "INSERT INTO vnn_real_rej VALUES (1, ARRAY[1.0, NULL, 3.0])" ),
                            "Expected rejection of a null element in a NOT NULL REAL ARRAY" );
                } finally {
                    statement.executeUpdate( "DROP TABLE vnn_real_rej" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    void realVectorPreparedStatementWithNullElementIsRejected() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE vnn_real_ps( id INTEGER NOT NULL, vec REAL NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                connection.commit();
            }
            try ( PreparedStatement ps = connection.prepareStatement( "INSERT INTO vnn_real_ps VALUES (?, ?)" ) ) {
                try {
                    ps.setInt( 1, 1 );
                    ps.setArray( 2, connection.createArrayOf( "REAL", new Float[]{ 1.0f, null, 3.0f } ) );
                    Assertions.assertThrows(
                            PrismInterfaceServiceException.class,
                            ps::executeUpdate,
                            "Expected rejection of a null element via PreparedStatement in a NOT NULL REAL ARRAY"
                    );
                } finally {
                    try ( Statement drop = connection.createStatement() ) {
                        drop.executeUpdate( "DROP TABLE vnn_real_ps" );
                        connection.commit();
                    }
                }
            }
        }
    }


    //--------------------- BOOLEAN NOT NULL ARRAY(1,n) ---------------------
    @Test
    void booleanVectorInsertWithAllNonNullElementsSucceeds() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate(
                        "CREATE TABLE vnn_bool( id INTEGER NOT NULL, vec BOOLEAN NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                try {
                    statement.executeUpdate( "INSERT INTO vnn_bool VALUES (1, ARRAY[TRUE, FALSE, TRUE])" );
                    connection.commit();
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT vec FROM vnn_bool WHERE id = 1" ),
                            ImmutableList.of( new Object[]{ new Object[]{ true, false, true } } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE vnn_bool" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    void booleanVectorLiteralWithNullElementIsRejected() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate(
                        "CREATE TABLE vnn_bool_rej( id INTEGER NOT NULL, vec BOOLEAN NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                try {
                    Assertions.assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "INSERT INTO vnn_bool_rej VALUES (1, ARRAY[TRUE, NULL, FALSE])" ),
                            "Expected rejection of a null element in a NOT NULL BOOLEAN ARRAY"
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE vnn_bool_rej" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    void booleanVectorPreparedStatementWithNullElementIsRejected() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE vnn_bool_ps( id INTEGER NOT NULL, vec BOOLEAN NOT NULL ARRAY(1,3), PRIMARY KEY (id) )" );
                connection.commit();
            }
            try ( PreparedStatement ps = connection.prepareStatement( "INSERT INTO vnn_bool_ps VALUES (?, ?)" ) ) {
                try {
                    ps.setInt( 1, 1 );
                    ps.setArray( 2, connection.createArrayOf( "BOOLEAN", new Boolean[]{ true, null, false } ) );
                    Assertions.assertThrows(
                            PrismInterfaceServiceException.class,
                            ps::executeUpdate,
                            "Expected rejection of a null element via PreparedStatement in a NOT NULL BOOLEAN ARRAY" );
                } finally {
                    try ( Statement drop = connection.createStatement() ) {
                        drop.executeUpdate( "DROP TABLE vnn_bool_ps" );
                        connection.commit();
                    }
                }
            }
        }
    }


    @Test
    void regularNullableRealArrayAllowsNullElements() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE vnn_real_nullable( id INTEGER NOT NULL, vec REAL ARRAY(1,3), PRIMARY KEY (id) )" );
                try {
                    statement.executeUpdate( "INSERT INTO vnn_real_nullable VALUES (1, ARRAY[1.0, NULL, 3.0])" );
                    connection.commit();
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM vnn_real_nullable WHERE id = 1" ),
                            ImmutableList.of( new Object[]{ 1 } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE vnn_real_nullable" );
                    connection.commit();
                }
            }
        }
    }

}

