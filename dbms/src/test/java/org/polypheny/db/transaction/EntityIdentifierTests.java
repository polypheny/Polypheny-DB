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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.transaction.locking.IdentifierUtils;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class EntityIdentifierTests {

    private static TestHelper testHelper;
    private static String identifierKey;


    @BeforeAll
    public static void start() throws SQLException {
        testHelper = TestHelper.getInstance();
        identifierKey = IdentifierUtils.IDENTIFIER_KEY;
    }


    @Test
    public void testCreateTable() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertUnparameterizedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES (1, 2)" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertPreparedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO identifiers (a, b) VALUES (?, ?)" );
                    preparedStatement.setInt( 1, 1 );
                    preparedStatement.setInt( 2, 2 );
                    preparedStatement.executeUpdate();
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleUnparameterizedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers (a, b) VALUES (1, 2), (3, 4)" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultiplePreparedWithColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO identifiers (a, b) VALUES (?, ?)" );
                    preparedStatement.setInt( 1, 1 );
                    preparedStatement.setInt( 2, 2 );
                    preparedStatement.addBatch();
                    preparedStatement.setInt( 1, 3 );
                    preparedStatement.setInt( 2, 4 );
                    preparedStatement.executeBatch();
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    // This currently fails due to a mismatch between the column names in the table which are mistakenly compared somehow
    @Test
    public void testInsertFromTableWithDifferentColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES (1, 2), (3, 4)" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x INTEGER NOT NULL, y INTEGER, PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers2 (x, y) SELECT a, b FROM identifiers1" );

                    //TODO TH: check that new identifiers had been assigned instead of copying from the first table
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    statement.executeUpdate( "DROP TABLE identifiers2" );
                    connection.commit();
                }
            }
        }
    }

    @Test
    public void testInsertFromTableWithoutTargetColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES (1, 2), (3, 4)" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x INTEGER NOT NULL, y INTEGER, PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 SELECT a, b FROM identifiers2" );

                    //TODO TH: check that new identifiers had been assigned instead of copying from the first table
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    statement.executeUpdate( "DROP TABLE identifiers2" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertFromTableWithoutColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES (1, 2), (3, 4)" );

                    statement.executeUpdate( "CREATE TABLE identifiers2 (x INTEGER NOT NULL, y INTEGER, PRIMARY KEY (x))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 SELECT * FROM identifiers2" );

                    //TODO TH: check that new identifiers had been assigned instead of copying from the first table
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    statement.executeUpdate( "DROP TABLE identifiers2" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertUnparameterizedDefaultExplicit() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES (1, DEFAULT)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertUnparameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a) VALUES (1)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertUnparameterizedDefaultExplicitNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 VALUES (1, DEFAULT)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertParameterizedDefaultExplicit() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 (a, b) VALUES (?, DEFAULT)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertParameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 (a) VALUES (?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertParameterizedDefaultExplicitNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    String insertSql = "INSERT INTO identifiers1 VALUES (?, DEFAULT)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 ); // Set value for 'a'
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleUnparameterizedDefaultExplicit() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a, b) VALUES (1, DEFAULT), (2, 3), (3, DEFAULT)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleUnparameterizedDefaultExplicitNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 VALUES (1, DEFAULT), (2, 3), (3, DEFAULT)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleUnparameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers1 (a) VALUES (1), (2), (3)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleParameterizedDefaultExplicit() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 (a, b) VALUES (?, DEFAULT), (?, ?), (?, DEFAULT)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 );
                        preparedStatement.setInt( 2, 2 );
                        preparedStatement.setInt( 3, 3 );
                        preparedStatement.setInt( 4, 3 );
                        preparedStatement.executeUpdate();
                    }

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleParameterizedDefaultExplicitNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 VALUES (?, DEFAULT), (?, ?), (?, DEFAULT)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 );
                        preparedStatement.setInt( 2, 2 );
                        preparedStatement.setInt( 3, 3 );
                        preparedStatement.setInt( 4, 3 );
                        preparedStatement.executeUpdate();
                    }

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertMultipleParameterizedDefaultOmitted() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers1 (a INTEGER NOT NULL, b INTEGER DEFAULT 42, PRIMARY KEY (a))" );

                    String insertSql = "INSERT INTO identifiers1 (a) VALUES (?), (?), (?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 );
                        preparedStatement.setInt( 2, 2 );
                        preparedStatement.setInt( 3, 3 );
                        preparedStatement.executeUpdate();
                    }

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers1" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertUnparameterizedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES (1, 2)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertPreparedNoColumnNames() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    String insertSql = "INSERT INTO table_name VALUES (?, ?)";
                    try ( PreparedStatement preparedStatement = connection.prepareStatement( insertSql ) ) {
                        preparedStatement.setInt( 1, 1 );
                        preparedStatement.setInt( 2, 2 );
                        preparedStatement.executeUpdate();
                    }
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE table_name" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testInsertUnparameterizedColumnNameConflictSameType() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
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
    //TODO TH: should fail
    public void testInsertUnparameterizedColumnNameConflictDifferentType() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
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
    //TODO TH: should fail
    public void testInsertUnparameterizedIdentifierManipulationInsert() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES (1, 2, 3)" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testUpdateUnparameterizedIdentifierManipulation() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES (1, 2)" );
                    statement.executeUpdate( "UPDATE identifiers SET _eid = 1 WHERE a = 1 AND b = 2" );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testDropIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES (1, 2)" );
                    statement.executeUpdate( "ALTER TABLE identifiers DROP COLUMN _eid" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testAddIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers DROP COLUMN _eid" ); // this should already fail here
                    statement.executeUpdate( "ALTER TABLE identifiers ADD COLUMN _eid BIGINT" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testDropMulitpleColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "INSERT INTO identifiers VALUES (1, 2)" );
                    statement.executeUpdate( "ALTER TABLE identifiers DROP COLUMN _eid DROP COLUMN b" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testAddMulitpleColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers ADD COLUMN _eid BIGINT ADD COLUMN c INTEGER" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testRenameIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers RENAME COLUMN _eid TO nowItsBroken" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    //TODO TH: should fail
    public void testChangeDataTypeOfIdentifierColumnUnparameterized() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE identifiers (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );
                    statement.executeUpdate( "ALTER TABLE identifiers MODIFY COLUMN _eid SET TYPE VARCHAR(15)" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE identifiers" );
                    connection.commit();
                }
            }
        }
    }


}
