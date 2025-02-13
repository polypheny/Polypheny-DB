/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class RelationalIdentifierTests {

    @BeforeAll
    static void setup() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                statement.execute( "CREATE RELATIONAL NAMESPACE test CONCURRENCY MVCC" );
                connection.commit();
            }
        }
    }


    @AfterAll
    static void teardown() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP NAMESPACE IF EXISTS test" );
                connection.commit();
            }
        }
    }


    @Test
    public void exceptionCheck() {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> IdentifierUtils.throwIfIsDisallowedKey( IdentifierUtils.IDENTIFIER_KEY )
        );
        assertTrue( exception.getMessage().contains( "are reserved for internal use and cannot be used" ) );
    }


    @Test
    public void testNonMvccCreateNamespace() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE RELATIONAL NAMESPACE nonMvccTest" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP NAMESPACE IF EXISTS nonMvccTest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccCreateTable() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccCreateTable2() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE _eid (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS _eid" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertUnparameterizedColumnNameConflictSameType() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (_eid BIGINT, a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertUnparameterizedColumnNameConflictDifferentType() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (_eid VARCHAR(15), a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    // TODO TH: fix this
    public void testNonMvccUpdateUnparameterizedColumnWithIdentifierName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, _eid INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, _eid) VALUES (1, 2)" );
                    statement.executeUpdate( "UPDATE identifiers SET _eid = 32 WHERE _eid = 2" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccDropColumnWithIdentifierName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, _eid INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers DROP COLUMN _eid" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccAddColumnWithIdentifierName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers ADD COLUMN _eid BIGINT" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccRenameColumnWithIdentifierName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, _eid INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers RENAME COLUMN _eid TO thisShouldWork" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccRenameNonIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers RENAME COLUMN b TO _eid" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccChangeDataTypeOfColumnWithIdentifierName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, _eid INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers MODIFY COLUMN _eid SET TYPE VARCHAR(15)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccCreateTableWithColumnWithIdentifierName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, _eid INTEGER, PRIMARY KEY (a))" );
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    /*
    Modify <- Project <- RelValues ('first', 'second')
     */
    @Test
    public void testNonMvccInsertUnparameterizedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES ('first', 'second')" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertMultipleUnparameterizedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES ('first', 'second'), ('third', 'fourth')" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    /*
    Modify <- Project <- RelValues ('first', 'second')
     */
    @Test
    public void testNonMvccInsertUnparameterizedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES ('first', 'second')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertMultipleUnparameterizedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES ('first', 'second'), ('third', 'fourth')" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    /*
    Modify <- Project <- Select ('_eid', first', 'second')
    */
    @Test
    public void testNonMvccInsertValuesMismatch() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "INSERT INTO identifiers VALUES (-32, 2, 3)" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertFromTableWithColumnNamesSameStore() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES ('first', 'second'), ('third', 'fourth')" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x VARCHAR(8) NOT NULL, y VARCHAR(8), PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers2 (x, y) SELECT a, b FROM identifiers1" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers2" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertFromTableWithoutColumnNamesSameStore() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES ('first', 'second'), ('third', 'fourth')" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x VARCHAR(8) NOT NULL, y VARCHAR(8), PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers2 SELECT a, b FROM identifiers1" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers2" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertUnparameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a) VALUES ('first')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertUnparameterizedDefaultExplicitNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 VALUES ('first', 'second')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertMultipleUnparameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a) VALUES ('first'), ('second'), ('third')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertPreparedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO identifiers (a, b) VALUES (?, ?)" );
                    preparedStatement.setString( 1, "first" );
                    preparedStatement.setString( 2, "second" );
                    preparedStatement.executeUpdate();
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertMultiplePreparedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO identifiers (a, b) VALUES (?, ?)" );
                    preparedStatement.setString( 1, "first" );
                    preparedStatement.setString( 2, "second" );
                    preparedStatement.addBatch();
                    preparedStatement.setString( 1, "third" );
                    preparedStatement.setString( 2, "fourth" );
                    preparedStatement.executeBatch();
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertParameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 (a) VALUES (?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setString( 1, "first" );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testNonMvccInsertPreparedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "public" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    String insertSql = "INSERT INTO identifiers VALUES (?, ?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setString( 1, "first" );
                        preparedStatement.setString( 2, "second" );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccCreateNamespace() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE RELATIONAL NAMESPACE mvccTest CONCURRENCY MVCC" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP NAMESPACE IF EXISTS mvccTest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccCreateTable() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccCreateTable2() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE _eid (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS _eid" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertUnparameterizedColumnNameConflictSameType() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "CREATE TABLE identifiers (_eid BIGINT, a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertUnparameterizedColumnNameConflictDifferentType() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "CREATE TABLE identifiers (_eid VARCHAR(15), a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccUpdateUnparameterizedIdentifierManipulation() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES (1, 2)" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "UPDATE identifiers SET _eid = 32 WHERE a = 1 AND b = 2" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccDropIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "ALTER TABLE identifiers DROP COLUMN _eid" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccAddIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "ALTER TABLE identifiers ADD COLUMN _eid BIGINT" )
                    );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccRenameIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "ALTER TABLE identifiers RENAME COLUMN _eid TO nowItsBroken" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccRenameNonIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "ALTER TABLE identifiers RENAME COLUMN b TO _eid" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccChangeDataTypeOfIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "ALTER TABLE identifiers MODIFY COLUMN _eid SET TYPE VARCHAR(15)" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccCreateTableIllegalColumnName() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, _eid INTEGER, PRIMARY KEY (a))" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    /*
    Modify <- Project <- RelValues ('first', 'second')
     */
    @Test
    public void testMvccInsertUnparameterizedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES ('first', 'second')" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertMultipleUnparameterizedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES ('first', 'second'), ('third', 'fourth')" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    /*
    Modify <- Project <- RelValues ('first', 'second')
     */
    @Test
    public void testMvccInsertUnparameterizedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES ('first', 'second')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertMultipleUnparameterizedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES ('first', 'second'), ('third', 'fourth')" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    /*
    Modify <- Project <- Select ('_eid', first', 'second')
    */
    @Test
    public void testMvccInsertUnparameterizedIdentifierManipulation() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    assertThrows(
                            PrismInterfaceServiceException.class,
                            () -> statement.executeUpdate( "INSERT INTO identifiers VALUES (-32, 2, 3)" )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertFromTableWithColumnNamesSameStore() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES ('first', 'second'), ('third', 'fourth')" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x VARCHAR(8) NOT NULL, y VARCHAR(8), PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers2 (x, y) SELECT a, b FROM identifiers1" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers2" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertFromTableWithoutColumnNamesSameStore() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES ('first', 'second'), ('third', 'fourth')" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x VARCHAR(8) NOT NULL, y VARCHAR(8), PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers2 SELECT a, b FROM identifiers1" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers2" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertUnparameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a) VALUES ('first')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertUnparameterizedDefaultExplicitNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 VALUES ('first', 'second')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertMultipleUnparameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a) VALUES ('first'), ('second'), ('third')" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertPreparedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO identifiers (a, b) VALUES (?, ?)" );
                    preparedStatement.setString( 1, "first" );
                    preparedStatement.setString( 2, "second" );
                    preparedStatement.executeUpdate();
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertMultiplePreparedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO identifiers (a, b) VALUES (?, ?)" );
                    preparedStatement.setString( 1, "first" );
                    preparedStatement.setString( 2, "second" );
                    preparedStatement.addBatch();
                    preparedStatement.setString( 1, "third" );
                    preparedStatement.setString( 2, "fourth" );
                    preparedStatement.executeBatch();
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertParameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a VARCHAR(8) NOT NULL, b VARCHAR(8) DEFAULT 'foo', PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 (a) VALUES (?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setString( 1, "first" );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testMvccInsertPreparedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( "test" );
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a VARCHAR(8) NOT NULL, b VARCHAR(8), PRIMARY KEY (a))" );
                    String insertSql = "INSERT INTO identifiers VALUES (?, ?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setString( 1, "first" );
                        preparedStatement.setString( 2, "second" );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


}
