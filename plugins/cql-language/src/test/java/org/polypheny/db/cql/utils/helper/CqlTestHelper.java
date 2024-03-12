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

package org.polypheny.db.cql.utils.helper;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
@Slf4j
public class CqlTestHelper {

    static boolean interfaceRunning = false;


    @BeforeAll
    public static void setup() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        if ( !interfaceRunning ) {
            deployCqlInterface();
            interfaceRunning = true;
        }

        createTestSchema();
        addTestData();
    }


    @AfterAll
    public static void teardown() {
        deleteTestData();
        //removeCqlInterface();
    }


    private static void deployCqlInterface() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER INTERFACES ADD \"cql\" USING 'org.polypheny.db.http.HttpInterface' WITH '{\"port\":\"8087\"}'" );
                connection.commit();
            }
        }
    }


    private static void removeCqlInterface() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER INTERFACES DROP \"cql\"" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while removing cql query interface", e );
        }
    }


    private static void createTestSchema() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA test" );
                statement.executeUpdate( "CREATE TABLE test.testtable( "
                        + "tbigint BIGINT NOT NULL, "
                        + "tboolean BOOLEAN NOT NULL, "
                        + "tdate DATE NOT NULL, "
                        + "tdecimal DECIMAL(5,2) NOT NULL, "
                        + "tdouble DOUBLE NOT NULL, "
                        + "tinteger INTEGER NOT NULL, "
                        + "treal REAL NOT NULL, "
                        + "tsmallint SMALLINT NOT NULL, "
                        + "ttinyint TINYINT NOT NULL, "
                        + "tvarchar VARCHAR(20) NOT NULL, "
                        + "PRIMARY KEY (tinteger) )" );
                statement.executeUpdate( "CREATE TABLE test.dept( "
                        + "deptno TINYINT NOT NULL, "
                        + "deptname VARCHAR(30) NOT NULL, "
                        + "PRIMARY KEY (deptno) )" );
                statement.executeUpdate( "CREATE TABLE test.employee( "
                        + "empno INTEGER NOT NULL, "
                        + "empname VARCHAR(20) NOT NULL, "
                        + "salary REAL NOT NULL, "
                        + "deptno TINYINT NOT NULL, "
                        + "married BOOLEAN NOT NULL, "
                        + "dob DATE NOT NULL, "
                        + "joining_date DATE NOT NULL, "
                        + "PRIMARY KEY (empno) )" );
                connection.commit();
            }
        }
    }


    private static void addTestData() {
        Random random = new Random();

//        Populate test.testtable with data
        insertIntoTestTable( 11111L, true, Date.valueOf( "1990-10-11" ),
                0.1111, 0.1111, 1, 11.1111, 1111,
                11, "Po" );
        insertIntoTestTable( 22222L, true, Date.valueOf( "2000-1-19" ),
                0.2222, 0.2222, 2, 22.2222, 2222,
                22, "ly" );
        insertIntoTestTable( 33333L, true, Date.valueOf( "2990-10-11" ),
                0.3333, 0.3333, 3, 33.3333, 3333,
                33, "ph" );
        insertIntoTestTable( 44444L, true, Date.valueOf( "2000-1-19" ),
                0.4444, 0.4444, 4, 44.4444, 4444,
                44, "en" );
        insertIntoTestTable( 55555L, true, Date.valueOf( "1990-1-11" ),
                0.5555, 0.5555, 5, 55.5555, 5555,
                55, "y-" );
        insertIntoTestTable( 66666L, true, Date.valueOf( "2000-10-14" ),
                0.6666, 0.6666, 6, 66.6666, 6666,
                66, "DB" );

//        Populate test.dept
        insertIntoDept( 1, "Human Resources" );
        insertIntoDept( 2, "Marketing" );
        insertIntoDept( 3, "Production" );
        insertIntoDept( 4, "Research and Development" );
        insertIntoDept( 5, "Accounting and Finance" );
        insertIntoDept( 6, "IT" );

//        Populate test.employee
        insertIntoEmployee( 1, "Joe", 10000, 1, true,
                Date.valueOf( "1970-2-3" ), Date.valueOf( "1990-6-1" ) );
        insertIntoEmployee( 2, "Amy", 25000, 1, false,
                Date.valueOf( "1977-6-3" ), Date.valueOf( "2001-3-7" ) );
        insertIntoEmployee( 3, "Charlie", 16000, 1, true,
                Date.valueOf( "1980-4-12" ), Date.valueOf( "2000-2-18" ) );
        insertIntoEmployee( 4, "Ravi", 40000, 1, false,
                Date.valueOf( "1970-11-11" ), Date.valueOf( "1990-12-17" ) );
        insertIntoEmployee( 5, "Imane", 27000, 2, true,
                Date.valueOf( "1989-3-3" ), Date.valueOf( "2001-11-1" ) );
        insertIntoEmployee( 6, "Rhody", 67500, 2, false,
                Date.valueOf( "1970-7-21" ), Date.valueOf( "1996-11-12" ) );
        insertIntoEmployee( 7, "Cryer", 17000, 2, true,
                Date.valueOf( "1969-12-31" ), Date.valueOf( "1990-1-1" ) );
        insertIntoEmployee( 8, "Lily", 34000, 2, false,
                Date.valueOf( "1980-2-3" ), Date.valueOf( "2002-3-1" ) );
        insertIntoEmployee( 9, "Rose", 15000, 3, true,
                Date.valueOf( "1983-6-13" ), Date.valueOf( "2008-1-12" ) );
        insertIntoEmployee( 10, "Happy", 19000, 3, false,
                Date.valueOf( "1965-9-16" ), Date.valueOf( "1990-9-16" ) );
        insertIntoEmployee( 11, "Marcus", 80000, 3, true,
                Date.valueOf( "1966-12-12" ), Date.valueOf( "1990-1-15" ) );
        insertIntoEmployee( 12, "Mary", 60000, 3, false,
                Date.valueOf( "1950-12-31" ), Date.valueOf( "1970-4-23" ) );
        insertIntoEmployee( 13, "Joy", 65000, 4, true,
                Date.valueOf( "1979-3-19" ), Date.valueOf( "2006-4-1" ) );
        insertIntoEmployee( 14, "Debby", 50000, 4, false,
                Date.valueOf( "1969-4-30" ), Date.valueOf( "1999-1-1" ) );
        insertIntoEmployee( 15, "Troy", 55000, 4, true,
                Date.valueOf( "1999-5-1" ), Date.valueOf( "2021-8-16" ) );
        insertIntoEmployee( 16, "Roy", 57000, 4, false,
                Date.valueOf( "1966-2-28" ), Date.valueOf( "1991-3-12" ) );
        insertIntoEmployee( 17, "Rich", 45000, 5, true,
                Date.valueOf( "1999-11-23" ), Date.valueOf( "2019-4-1" ) );
        insertIntoEmployee( 18, "Anagha", 90000, 5, false,
                Date.valueOf( "2000-10-14" ), Date.valueOf( "2021-10-14" ) );
        insertIntoEmployee( 19, "Diana", 19700, 5, true,
                Date.valueOf( "1945-1-3" ), Date.valueOf( "1970-6-3" ) );
        insertIntoEmployee( 20, "Matt", 33000, 5, false,
                Date.valueOf( "1956-12-13" ), Date.valueOf( "1989-6-16" ) );
        insertIntoEmployee( 21, "Holt", 87000, 6, true,
                Date.valueOf( "1958-2-25" ), Date.valueOf( "1990-1-1" ) );
        insertIntoEmployee( 22, "Peralta", 22000, 6, false,
                Date.valueOf( "1980-1-31" ), Date.valueOf( "1990-1-1" ) );
        insertIntoEmployee( 23, "Mando", 35700, 6, true,
                Date.valueOf( "1967-11-30" ), Date.valueOf( "1990-1-1" ) );
        insertIntoEmployee( 24, "Vader", 3000, 6, false,
                Date.valueOf( "1959-12-13" ), Date.valueOf( "1990-1-1" ) );
    }


    private static void insertIntoTestTable(
            long tbigint,
            boolean tboolean,
            Date tdate,
            double tdecimal,
            double tdouble,
            int tinteger,
            double treal,
            int tsmallint,
            int ttinyint,
            String tvarchar ) {

        String query = String.format( Locale.ROOT, "INSERT INTO test.testtable (tbigint, tboolean, tdate,"
                        + " tdecimal, tdouble, tinteger, treal, tsmallint, ttinyint, tvarchar)"
                        + " VALUES (ROW(%d, %b, DATE '%s', %f, %f, %d, %f, %d, %d, '%s'))",
                tbigint, tboolean, tdate.toString(), tdecimal, tdouble, tinteger, treal, tsmallint, ttinyint, tvarchar );
        executeInsertion( query );
    }


    private static void insertIntoDept( int deptno, String deptname ) {
        String query = String.format( Locale.ROOT, "INSERT INTO test.dept (deptno, deptname) VALUES (%d, '%s')", deptno, deptname );
        executeInsertion( query );
    }


    private static void insertIntoEmployee( int empno, String empname, double salary, int deptno, boolean married, Date dob, Date joiningDate ) {
        String query = String.format( Locale.ROOT, "INSERT INTO test.employee (empno, empname, salary, deptno, married, dob,"
                        + " joining_date) VALUES (ROW(%d, '%s', %f, %d, %b, DATE '%s', DATE '%s'))",
                empno, empname, salary, deptno, married, dob.toString(), joiningDate.toString() );
        executeInsertion( query );
    }


    private static void executeInsertion( String query ) {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( query );
                    connection.commit();
                } catch ( SQLException e ) {
                    log.error( "Exception while inserting test data", e );
                }
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting test data", e );
        }
    }


    private static void deleteTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "DROP TABLE test.testtable" );
                    statement.executeUpdate( "DROP TABLE test.dept" );
                    statement.executeUpdate( "DROP TABLE test.employee" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting test data", e );
                }
                statement.executeUpdate( "DROP SCHEMA test" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting test data", e );
        }
    }

}
