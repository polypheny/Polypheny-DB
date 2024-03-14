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

package org.polypheny.db.sql.view;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

/*
 * Table and Queries from https://github.com/polypheny/OLTPBench/tree/polypheny/src/com/oltpbenchmark/benchmarks/tpch
 */
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class ComplexViewTest {

    private final static String DROP_TABLES_NATION = "DROP TABLE IF EXISTS nation";
    private final static String DROP_TABLES_REGION = "DROP TABLE IF EXISTS region";
    private final static String DROP_TABLES_PART = "DROP TABLE IF EXISTS part";
    private final static String DROP_TABLES_SUPPLIER = "DROP TABLE IF EXISTS supplier";
    private final static String DROP_TABLES_PARTSUPP = "DROP TABLE IF EXISTS partsupp";
    private final static String DROP_TABLES_ORDERS = "DROP TABLE IF EXISTS orders";
    private final static String DROP_TABLES_CUSTOMER = "DROP TABLE IF EXISTS customer";
    private final static String DROP_TABLES_LINEITEM = "DROP TABLE IF EXISTS lineitem";

    private final static String NATION_TABLE = "CREATE TABLE nation ( "
            + "n_nationkey  INTEGER NOT NULL,"
            + "n_name VARCHAR(25) NOT NULL,"
            + "n_regionkey INTEGER NOT NULL,"
            + "n_comment VARCHAR(152),"
            + "PRIMARY KEY (n_nationkey) )";

    private final static String NATION_TABLE_DATA = "INSERT INTO nation VALUES ("
            + "1,"
            + "'Switzerland',"
            + "1,"
            + "'nice'"
            + ")";

    private final static Object[] NATION_TEST_DATA = new Object[]{
            1,
            "Switzerland",
            1,
            "nice" };

    private final static String REGION_TABLE = "CREATE TABLE region  ( "
            + "r_regionkey INTEGER NOT NULL,"
            + "r_name VARCHAR(25) NOT NULL,"
            + "r_comment VARCHAR(152),"
            + "PRIMARY KEY (r_regionkey) )";

    private final static String REGION_TABLE_DATA = "INSERT INTO region VALUES ("
            + "1,"
            + "'Basel',"
            + "'nice'"
            + ")";

    private final static Object[] REGION_TEST_DATA = new Object[]{
            1,
            "Basel",
            "nice" };

    private final static String PART_TABLE = "CREATE TABLE part ( "
            + "p_partkey INTEGER NOT NULL,"
            + "p_name VARCHAR(55) NOT NULL,"
            + "p_mfgr VARCHAR(25) NOT NULL,"
            + "p_brand VARCHAR(10) NOT NULL,"
            + "p_type VARCHAR(25) NOT NULL,"
            + "p_size INTEGER NOT NULL,"
            + "p_container VARCHAR(10) NOT NULL,"
            + "p_retailprice DECIMAL(15,2) NOT NULL,"
            + "p_comment VARCHAR(23) NOT NULL,"
            + "PRIMARY KEY (p_partkey) )";

    private final static String PART_TABLE_DATA = "INSERT INTO part VALUES ("
            + "1,"
            + "'Mouse',"
            + "'mfgr',"
            + "'Logitec',"
            + "'Wireless',"
            + "5,"
            + "'container',"
            + "65.00,"
            + "'black'"
            + ")";

    private final static Object[] PART_TEST_DATA = new Object[]{
            1,
            "Mouse",
            "mfgr",
            "Logitec",
            "Wireless",
            5,
            "container",
            new BigDecimal( "65.00" ),
            "black" };

    private final static String SUPPLIER_TABLE = "CREATE TABLE supplier ( "
            + "s_suppkey INTEGER NOT NULL,"
            + "s_name VARCHAR(25) NOT NULL,"
            + "s_address VARCHAR(40) NOT NULL,"
            + "s_nationkey INTEGER NOT NULL,"
            + "s_phone VARCHAR(15) NOT NULL,"
            + "s_acctbal DECIMAL(15,2) NOT NULL,"
            + "s_comment VARCHAR(101) NOT NULL,"
            + "PRIMARY KEY (s_suppkey) )";

    private final static String SUPPLIER_TABLE_DATA = "INSERT INTO supplier VALUES ("
            + "1,"
            + "'SupplierName',"
            + "'SupplierAddress',"
            + "1,"
            + "'phone',"
            + "5.15,"
            + "'SupplierComment'"
            + ")";

    private final static Object[] SUPPLIER_TEST_DATA = new Object[]{
            1,
            "SupplierName",
            "SupplierAddress",
            1,
            "phone",
            new BigDecimal( "5.15" ),
            "SupplierComment" };

    private final static String PARTSUPP_TABLE = "CREATE TABLE partsupp ( "
            + "ps_partkey INTEGER NOT NULL,"
            + "ps_suppkey INTEGER NOT NULL,"
            + "ps_availqty INTEGER NOT NULL,"
            + "ps_supplycost DECIMAL(15,2)  NOT NULL,"
            + "ps_comment VARCHAR(199) NOT NULL,"
            + "PRIMARY KEY (ps_partkey, ps_suppkey) )";

    private final static String PARTSUPP_TABLE_DATA = "INSERT INTO partsupp VALUES ("
            + "1,"
            + "1,"
            + "7,"
            + "25.15,"
            + "'black'"
            + ")";

    private final static Object[] PARTSUPP_TEST_DATA = new Object[]{
            1,
            1,
            7,
            new BigDecimal( "25.15" ),
            "black" };

    private final static String CUSTOMER_TABLE = "CREATE TABLE customer ("
            + "c_custkey INTEGER NOT NULL,"
            + "c_name VARCHAR(25) NOT NULL,"
            + "c_address VARCHAR(40) NOT NULL,"
            + "c_nationkey INTEGER NOT NULL,"
            + "c_phone VARCHAR(15) NOT NULL,"
            + "c_acctbal DECIMAL(15,2) NOT NULL,"
            + "c_mktsegment VARCHAR(10) NOT NULL,"
            + "c_comment  VARCHAR(117) NOT NULL,"
            + " PRIMARY KEY (c_custkey) )";

    private final static String CUSTOMER_TABLE_DATA = "INSERT INTO customer VALUES ( "
            + "1,"
            + "'CName',"
            + "'CAddress',"
            + "1,"
            + "'CPhone',"
            + "5.15,"
            + "'CSegment',"
            + "'nice'"
            + ")";

    private final static Object[] CUSTOMER_TEST_DATA = new Object[]{
            1,
            "CName",
            "CAddress",
            1,
            "CPhone",
            new BigDecimal( "5.15" ),
            "CSegment",
            "nice" };

    private final static String ORDERS_TABLE = "CREATE TABLE orders ( "
            + "o_orderkey INTEGER NOT NULL,"
            + "o_custkey INTEGER NOT NULL,"
            + "o_orderstatus VARCHAR(1) NOT NULL,"
            + "o_totalprice DECIMAL(15,2) NOT NULL,"
            + "o_orderdate DATE NOT NULL,"
            + "o_orderpriority VARCHAR(15) NOT NULL,"
            + "o_clerk VARCHAR(15) NOT NULL,"
            + "o_shippriority INTEGER NOT NULL,"
            + "o_comment VARCHAR(79) NOT NULL,"
            + "PRIMARY KEY (o_orderkey) )";

    private final static String ORDERS_TABLE_DATA = "INSERT INTO orders VALUES ("
            + "1,"
            + "1,"
            + "'A',"
            + "65.15,"
            + "date '2020-07-03',"
            + "'orderPriority',"
            + "'clerk',"
            + "1,"
            + "'fast'"
            + ")";

    private final static Object[] ORDERS_TEST_DATA = new Object[]{
            1,
            1,
            "A",
            new BigDecimal( "65.15" ),
            Date.valueOf( "2020-07-03" ),
            "orderPriority",
            "clerk",
            1,
            "fast" };

    public final static String LINEITEM_TABLE = "CREATE TABLE lineitem ( "
            + "l_orderkey INTEGER NOT NULL,"
            + "l_partkey INTEGER NOT NULL,"
            + "l_suppkey INTEGER NOT NULL,"
            + "l_linenumber INTEGER NOT NULL,"
            + "l_quantity DECIMAL(15,2) NOT NULL,"
            + "l_extendedprice DECIMAL(15,2) NOT NULL,"
            + "l_discount DECIMAL(15,2) NOT NULL,"
            + "l_tax DECIMAL(15,2) NOT NULL,"
            + "l_returnflag VARCHAR(1) NOT NULL,"
            + "l_linestatus VARCHAR(1) NOT NULL,"
            + "l_shipdate DATE NOT NULL,"
            + "l_commitdate DATE NOT NULL,"
            + "l_receiptdate DATE NOT NULL,"
            + "l_shipinstruct VARCHAR(25) NOT NULL,"
            + "l_shipmode VARCHAR(10) NOT NULL,"
            + "l_comment VARCHAR(44) NOT NULL,"
            + "PRIMARY KEY (l_orderkey, l_linenumber) )";

    private final static String LINEITEM_TABLE_DATA = "INSERT INTO lineitem VALUES ("
            + "1,"
            + "1,"
            + "1,"
            + "1,"
            + "20.15,"
            + "50.15,"
            + "20.15,"
            + "10.15,"
            + "'R',"
            + "'L',"
            + "date '2020-07-03',"
            + "date '2020-07-03',"
            + "date '2020-09-03',"
            + "'shipingstruct',"
            + "'mode',"
            + "'shipingComment'"
            + ")";

    private final static Object[] LINEITEM_TEST_DATA = new Object[]{
            1,
            1,
            1,
            1,
            new BigDecimal( "20.15" ),
            new BigDecimal( "50.15" ),
            new BigDecimal( "20.15" ),
            new BigDecimal( "10.15" ),
            "R",
            "L",
            Date.valueOf( "2020-07-03" ),
            Date.valueOf( "2020-07-03" ),
            Date.valueOf( "2020-09-03" ),
            "shipingstruct",
            "mode",
            "shipingComment" };

    private final static Object[] date_TEST_DATA = new Object[]{
            Date.valueOf( "2020-07-03" ) };

    private final static Object[] decimal_TEST_DATA = new Object[]{
            new BigDecimal( "65.15" ) };

    private final static Object[] decimalDate_TEST_DATA = new Object[]{
            new BigDecimal( "65.15" ),
            Date.valueOf( "2020-07-03" ) };

    private final static Object[] decimalDateInt_TEST_DATA = new Object[]{
            new BigDecimal( "65.15" ),
            Date.valueOf( "2020-07-03" ),
            1 };

    private final static Object[] q1_TEST_DATA = new Object[]{
            "R",
            "L",
            new BigDecimal( "20.15" ),
            new BigDecimal( "50.15" ),
            new BigDecimal( "-960.3725" ),
            new BigDecimal( "-10708.153375" ),
            new BigDecimal( "20.15" ),
            new BigDecimal( "50.15" ),
            new BigDecimal( "20.15" ),
            1L };

    private final static Object[] q1_TEST_DATA_MAT = new Object[]{
            "R",
            "L",
            new BigDecimal( "20.15" ),
            new BigDecimal( "50.15" ),
            new BigDecimal( "-960.3725" ),
            new BigDecimal( "-10708.153375" ),
            new BigDecimal( "20.15" ),
            new BigDecimal( "50.15" ),
            new BigDecimal( "20.15" ),
            1L,
            0 };

    private final static Object[] q3_TEST_DATA = new Object[]{
            1,
            new BigDecimal( "-960.3725" ),
            Date.valueOf( "2020-07-03" ),
            1 };

    private final static Object[] q4_TEST_DATA = new Object[]{
            "orderPriority",
            1L };

    private final static Object[] q6_TEST_DATA = new Object[]{
            new BigDecimal( "1010.5225" ) };

    private final static Object[] q7_TEST_DATA = new Object[]{
            "Switzerland",
            "Switzerland",
            2020L,
            new BigDecimal( "-960.3725" ) };

    private final static Object[] q8_TEST_DATA = new Object[]{
            2020L,
            new BigDecimal( "1.0000" ) };

    private final static Object[] q8_TEST_DATA_VIEW = new Object[]{
            2020L,
            new BigDecimal( "1" ) };

    private final static Object[] q9_TEST_DATA = new Object[]{
            "Switzerland",
            2020L,
            new BigDecimal( "-1467.1450" ) };

    private final static Object[] q10_TEST_DATA = new Object[]{
            1,
            "CName",
            new BigDecimal( "-960.3725" ),
            new BigDecimal( "5.15" ),
            "Switzerland",
            "CAddress",
            "CPhone",
            "nice" };

    private final static Object[] q13_TEST_DATA = new Object[]{
            0L,
            1L
    };

    private final static Object[] q14_TEST_DATA = new Object[]{
            new BigDecimal( "100.00000000" )
    };

    private final static Object[] q15_TEST_DATA = new Object[]{
            1,
            "SupplierName",
            "SupplierAddress",
            "phone",
            new BigDecimal( "-960.3725" ) };

    private final static Object[] q17_TEST_DATA = new Object[]{
            new BigDecimal( "7.164285714285714" )
    };

    private final static Object[] q19_TEST_DATA = new Object[]{
            new BigDecimal( "-960.3725" )
    };


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    public void initTables( Statement statement ) throws SQLException {
        statement.executeUpdate( NATION_TABLE );
        statement.executeUpdate( NATION_TABLE_DATA );
        statement.executeUpdate( REGION_TABLE );
        statement.executeUpdate( REGION_TABLE_DATA );
        statement.executeUpdate( PART_TABLE );
        statement.executeUpdate( PART_TABLE_DATA );
        statement.executeUpdate( SUPPLIER_TABLE );
        statement.executeUpdate( SUPPLIER_TABLE_DATA );
        statement.executeUpdate( PARTSUPP_TABLE );
        statement.executeUpdate( PARTSUPP_TABLE_DATA );
        statement.executeUpdate( CUSTOMER_TABLE );
        statement.executeUpdate( CUSTOMER_TABLE_DATA );
        statement.executeUpdate( ORDERS_TABLE );
        statement.executeUpdate( ORDERS_TABLE_DATA );
        statement.executeUpdate( LINEITEM_TABLE );
        statement.executeUpdate( LINEITEM_TABLE_DATA );
    }


    public void dropTables( Statement statement ) throws SQLException {
        statement.executeUpdate( DROP_TABLES_NATION );
        statement.executeUpdate( DROP_TABLES_REGION );
        statement.executeUpdate( DROP_TABLES_PART );
        statement.executeUpdate( DROP_TABLES_SUPPLIER );
        statement.executeUpdate( DROP_TABLES_PARTSUPP );
        statement.executeUpdate( DROP_TABLES_ORDERS );
        statement.executeUpdate( DROP_TABLES_CUSTOMER );
        statement.executeUpdate( DROP_TABLES_LINEITEM );
    }


    @Test
    public void testPreparations() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );
                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM nation" ),
                            ImmutableList.of( NATION_TEST_DATA )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM region" ),
                            ImmutableList.of( REGION_TEST_DATA )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM part" ),
                            ImmutableList.of( PART_TEST_DATA )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM supplier" ),
                            ImmutableList.of( SUPPLIER_TEST_DATA )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM partsupp" ),
                            ImmutableList.of( PARTSUPP_TEST_DATA )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM customer" ),
                            ImmutableList.of( CUSTOMER_TEST_DATA )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM orders" ),
                            ImmutableList.of( ORDERS_TEST_DATA )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM lineitem" ),
                            ImmutableList.of( LINEITEM_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testDate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ORDERS_TABLE );
                statement.executeUpdate( ORDERS_TABLE_DATA );

                try {
                    String query = "SELECT o_orderdate FROM orders;";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( date_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW date_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM date_VIEW" ),
                            ImmutableList.of( date_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS date_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testDecimal() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ORDERS_TABLE );
                statement.executeUpdate( ORDERS_TABLE_DATA );

                try {
                    String query = "SELECT o_totalprice FROM orders";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( decimal_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW decimal_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM decimal_VIEW" ),
                            ImmutableList.of( decimal_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS decimal_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testDecimalDate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ORDERS_TABLE );
                statement.executeUpdate( ORDERS_TABLE_DATA );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "o_totalprice, "
                                    + "o_orderdate "
                                    + "from "
                                    + "orders " ),
                            ImmutableList.of( decimalDate_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW decimalDate_VIEW AS "
                            + "select "
                            + "o_totalprice, "
                            + "o_orderdate "
                            + "from "
                            + "orders " );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM decimalDate_VIEW" ),
                            ImmutableList.of( decimalDate_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS decimalDate_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testDecimalDateInt() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ORDERS_TABLE );
                statement.executeUpdate( ORDERS_TABLE_DATA );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "o_totalprice, "
                                    + "o_orderdate,"
                                    + "o_orderkey "
                                    + "from "
                                    + "orders " ),
                            ImmutableList.of( decimalDateInt_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW decimalDateInt_VIEW AS "
                            + "select "
                            + "o_totalprice, "
                            + "o_orderdate,"
                            + "o_orderkey "
                            + "from "
                            + "orders " );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM decimalDateInt_VIEW" ),
                            ImmutableList.of( decimalDateInt_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS decimalDateInt_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testDateOrderBy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE orders ( o_orderkey INTEGER NOT NULL, o_orderdate  DATE NOT NULL, PRIMARY KEY (o_orderkey) )" );
                statement.executeUpdate( "CREATE TABLE lineitem ( l_extendedprice  DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, PRIMARY KEY (l_extendedprice) )" );
                statement.executeUpdate( "INSERT INTO orders VALUES (1,date '2020-07-03')" );
                statement.executeUpdate( "INSERT INTO lineitem VALUES (20.15,50.15)" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate "
                                    + "from  orders, lineitem "
                                    + "where o_orderdate < date '2020-08-03' "
                                    + "group by o_orderdate "
                                    + "order by revenue desc, o_orderdate" ),
                            ImmutableList.of( new Object[]{
                                    new BigDecimal( "-990.3725" ),
                                    Date.valueOf( "2020-07-03" )
                            } )
                    );

                    statement.executeUpdate( "CREATE VIEW dateOrderby_VIEW AS "
                            + "select sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate "
                            + "from  orders, lineitem "
                            + "where o_orderdate < date '2020-08-03' "
                            + "group by o_orderdate "
                            + "order by revenue desc, o_orderdate" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM dateOrderby_VIEW" ),
                            ImmutableList.of( new Object[]{
                                    new BigDecimal( "-990.3725" ),
                                    Date.valueOf( "2020-07-03" )
                            } )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS dateOrderby_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testTimeInterval() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE orders (o_orderkey INTEGER NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority  VARCHAR(15) NOT NULL, PRIMARY KEY (o_orderkey) )" );
                statement.executeUpdate( "INSERT INTO orders VALUES (1, date '2020-07-03', 'orderPriority')" );
                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select o_orderpriority\n"
                                    + "from orders where o_orderdate < date '2020-07-03' + interval '3' month " ),
                            ImmutableList.of( new Object[]{
                                    "orderPriority"
                            } )
                    );

                    statement.executeUpdate( "CREATE VIEW timeIntervall_VIEW AS "
                            + "select o_orderpriority from orders where o_orderdate < date '2020-07-03' + interval '3' month" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM timeIntervall_VIEW" ),
                            ImmutableList.of( new Object[]{
                                    "orderPriority"
                            } )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS timeIntervall_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ1() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                l_returnflag,
                                l_linestatus,
                                SUM(l_quantity) AS sum_qty,
                                SUM(l_extendedprice) AS sum_base_price,
                                SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                                AVG(l_quantity) AS avg_qty,
                                AVG(l_extendedprice) AS avg_price,
                                AVG(l_discount) AS avg_disc,
                                COUNT(*) AS count_order
                            FROM
                                lineitem
                            WHERE
                                l_shipdate <= DATE '2020-07-03'
                            GROUP BY
                                l_returnflag,
                                l_linestatus
                            ORDER BY
                                l_returnflag,
                                l_linestatus""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q1_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q1_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q1_VIEW " ),
                            ImmutableList.of( q1_TEST_DATA )
                    );

                    statement.executeUpdate( """
                            CREATE MATERIALIZED VIEW q1_Materialized AS
                            SELECT
                                l_returnflag,
                                l_linestatus,
                                SUM(l_quantity) AS sum_qty,
                                SUM(l_extendedprice) AS sum_base_price,
                                SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                                AVG(l_quantity) AS avg_qty,
                                AVG(l_extendedprice) AS avg_price,
                                AVG(l_discount) AS avg_disc,
                                COUNT(*) AS count_order
                            FROM
                                lineitem
                            WHERE
                                l_shipdate <= DATE '2020-07-03'
                            GROUP BY
                                l_returnflag,
                                l_linestatus
                            ORDER BY
                                l_returnflag,
                                l_linestatus
                            FRESHNESS INTERVAL 10 "min";
                            """ );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q1_Materialized " ),
                            ImmutableList.of( q1_TEST_DATA_MAT )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP MATERIALIZED VIEW IF EXISTS q1_Materialized" );
                    statement.executeUpdate( "DROP VIEW IF EXISTS q1_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ2() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                s_acctbal,
                                s_name,
                                n_name,
                                p_partkey,
                                p_mfgr,
                                s_address,
                                s_phone,
                                s_comment
                            FROM
                                part
                            JOIN
                                partsupp ON p_partkey = ps_partkey
                            JOIN
                                supplier ON s_suppkey = ps_suppkey
                            JOIN
                                nation ON s_nationkey = n_nationkey
                            JOIN
                                region ON n_regionkey = r_regionkey
                            WHERE
                                p_size = 5
                                AND p_type LIKE 'Wireless'
                                AND r_name = 'Basel'
                                AND ps_supplycost = (
                                    SELECT
                                        MIN(ps_supplycost)
                                    FROM
                                        partsupp
                                    JOIN
                                        supplier ON s_suppkey = ps_suppkey
                                    JOIN
                                        nation ON s_nationkey = n_nationkey
                                    JOIN
                                        region ON n_regionkey = r_regionkey
                                    WHERE
                                        p_partkey = ps_partkey
                                        AND r_name = 'Basel'
                                )
                            ORDER BY
                                s_acctbal DESC,
                                n_name,
                                s_name,
                                p_partkey
                            LIMIT 100""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( new Object[]{ 5.15, "SupplierName", "Switzerland", 1, "mfgr", "SupplierAddress", "phone", "SupplierComment" } )
                    );
                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ3() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                l_orderkey,
                                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                                o_orderdate,
                                o_shippriority
                            FROM
                                customer
                            JOIN
                                orders ON c_custkey = o_custkey
                            JOIN
                                lineitem ON l_orderkey = o_orderkey
                            WHERE
                                c_mktsegment = 'CSegment'
                                AND o_orderdate < DATE '2020-08-03'
                                AND l_shipdate > DATE '2020-06-03'
                            GROUP BY
                                l_orderkey,
                                o_orderdate,
                                o_shippriority
                            ORDER BY
                                revenue DESC,
                                o_orderdate
                            LIMIT 10""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q3_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q3_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q3_VIEW" ),
                            ImmutableList.of( q3_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q3_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ4() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                o_orderpriority,
                                COUNT(*) AS order_count
                            FROM
                                orders
                            WHERE
                                o_orderdate >= DATE '2020-07-03'
                                AND o_orderdate < DATE '2020-07-03' + INTERVAL '3' MONTH
                                AND EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        lineitem
                                    WHERE
                                        l_orderkey = o_orderkey
                                        AND l_commitdate < l_receiptdate
                                )
                            GROUP BY
                                o_orderpriority
                            ORDER BY
                                o_orderpriority""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q4_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q4_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q4_VIEW" ),
                            ImmutableList.of( q4_TEST_DATA )
                    );

                    // statement.executeUpdate( "DROP VIEW q4_VIEW" );
                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q4_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    // Original TPC-H query with l_discount between 20.15 - 0.01 and ? + 0.01 does not return a result
    // changed to and l_discount between 20.14 and 20.16
    @Test
    public void testQ6() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                SUM(l_extendedprice * l_discount) AS revenue
                            FROM
                                lineitem
                            WHERE
                                l_shipdate >= DATE '2020-07-03'
                                AND l_shipdate < DATE '2020-07-03' + INTERVAL '1' YEAR
                                AND l_discount BETWEEN 20.14 AND 20.16
                                AND l_quantity < 20.20""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q6_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q6_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q6_VIEW" ),
                            ImmutableList.of( q6_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q6_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    // deleted "or (n1.n_name = ? and n2.n_name = ?) " because there is only one nation in this table
    @Test
    public void testQ7() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                supp_nation,
                                cust_nation,
                                l_year,
                                SUM(volume) AS revenue
                            FROM
                                (
                                    SELECT
                                        n1.n_name AS supp_nation,
                                        n2.n_name AS cust_nation,
                                        EXTRACT(YEAR FROM l_shipdate) AS l_year,
                                        l_extendedprice * (1 - l_discount) AS volume
                                    FROM
                                        supplier
                                    JOIN
                                        lineitem ON s_suppkey = l_suppkey
                                    JOIN
                                        orders ON o_orderkey = l_orderkey
                                    JOIN
                                        customer ON c_custkey = o_custkey
                                    JOIN
                                        nation n1 ON s_nationkey = n1.n_nationkey
                                    JOIN
                                        nation n2 ON c_nationkey = n2.n_nationkey
                                    WHERE
                                        n1.n_name = 'Switzerland'
                                        AND n2.n_name = 'Switzerland'
                                        AND l_shipdate BETWEEN DATE '2020-06-03' AND DATE '2020-08-03'
                                ) AS shipping
                            GROUP BY
                                supp_nation,
                                cust_nation,
                                l_year
                            ORDER BY
                                supp_nation,
                                cust_nation,
                                l_year""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q7_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q7_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q7_VIEW" ),
                            ImmutableList.of( q7_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q7_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ8() throws SQLException {
        assumeFalse( System.getProperty( "java.version" ).startsWith( "1.8" ) ); // Out of memory error on Java 8
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                O_YEAR,
                                SUM(CASE
                                    WHEN NATION = 'Switzerland' THEN VOLUME
                                    ELSE 0
                                END) / SUM(VOLUME) AS MKT_SHARE
                            FROM
                                (
                                SELECT
                                    EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR,
                                    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
                                    N2.N_NAME AS NATION
                                FROM
                                    PART,
                                    SUPPLIER,
                                    LINEITEM,
                                    ORDERS,
                                    CUSTOMER,
                                    NATION N1,
                                    NATION N2,
                                    REGION
                                WHERE
                                    P_PARTKEY = L_PARTKEY
                                    AND S_SUPPKEY = L_SUPPKEY
                                    AND L_ORDERKEY = O_ORDERKEY
                                    AND O_CUSTKEY = C_CUSTKEY
                                    AND C_NATIONKEY = N1.N_NATIONKEY
                                    AND N1.N_REGIONKEY = R_REGIONKEY
                                    AND R_NAME = 'Basel'
                                    AND S_NATIONKEY = N2.N_NATIONKEY
                                    AND O_ORDERDATE BETWEEN DATE '2020-06-03' AND DATE '2020-08-03'
                                    AND P_TYPE = 'Wireless'
                                ) AS ALL_NATIONS
                            GROUP BY
                                O_YEAR
                            ORDER BY
                                O_YEAR""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q8_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q8_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q8_VIEW" ),
                            ImmutableList.of( q8_TEST_DATA_VIEW )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q8_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ9() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                NATION,
                                O_YEAR,
                                SUM(AMOUNT) AS SUM_PROFIT
                            FROM
                                (
                                SELECT
                                    N_NAME AS NATION,
                                    EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR,
                                    L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
                                FROM
                                    PART,
                                    SUPPLIER,
                                    LINEITEM,
                                    PARTSUPP,
                                    ORDERS,
                                    NATION
                                WHERE
                                    S_SUPPKEY = L_SUPPKEY
                                    AND PS_SUPPKEY = L_SUPPKEY
                                    AND PS_PARTKEY = L_PARTKEY
                                    AND P_PARTKEY = L_PARTKEY
                                    AND O_ORDERKEY = L_ORDERKEY
                                    AND S_NATIONKEY = N_NATIONKEY
                                    AND P_NAME LIKE 'Mouse'
                                ) AS PROFIT
                            GROUP BY
                                NATION,
                                O_YEAR
                            ORDER BY
                                NATION,
                                O_YEAR DESC""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q9_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q9_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q9_VIEW" ),
                            ImmutableList.of( q9_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q9_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ10() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                c_custkey,
                                c_name,
                                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                                c_acctbal,
                                n_name,
                                c_address,
                                c_phone,
                                c_comment
                            FROM
                                customer
                            JOIN
                                orders ON c_custkey = o_custkey
                            JOIN
                                lineitem ON l_orderkey = o_orderkey
                            JOIN
                                nation ON c_nationkey = n_nationkey
                            WHERE
                                o_orderdate >= DATE '2020-07-03'
                                AND o_orderdate < DATE '2020-07-03' + INTERVAL '3' MONTH
                                AND l_returnflag = 'R'
                            GROUP BY
                                c_custkey,
                                c_name,
                                c_acctbal,
                                c_phone,
                                n_name,
                                c_address,
                                c_comment
                            ORDER BY
                                revenue DESC
                            LIMIT 20""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q10_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q10_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q10_VIEW" ),
                            ImmutableList.of( q10_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q10_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ11() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( """
                                    SELECT
                                        ps_partkey,
                                        SUM(ps_supplycost * ps_availqty) AS valueAA
                                    FROM
                                        partsupp
                                    JOIN
                                        supplier ON ps_suppkey = s_suppkey
                                    JOIN
                                        nation ON s_nationkey = n_nationkey
                                    WHERE
                                        n_name = 'Switzerland'
                                    GROUP BY
                                        ps_partkey
                                    HAVING
                                        SUM(ps_supplycost * ps_availqty) > (
                                            SELECT
                                                SUM(ps_supplycost * ps_availqty) * 0.00001
                                            FROM
                                                partsupp
                                            JOIN
                                                supplier ON ps_suppkey = s_suppkey
                                            JOIN
                                                nation ON s_nationkey = n_nationkey
                                            WHERE
                                                n_name = 'Switzerland'
                                        )
                                    ORDER BY
                                        valueAA DESC""" ),
                            ImmutableList.of( new Object[]{ 1, 176.05 } )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ12() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                l_shipmode,
                                SUM(CASE
                                    WHEN o_orderpriority = 'orderPriority' OR o_orderpriority = 'orderPriority1' THEN 1
                                    ELSE 0
                                END) AS high_line_count,
                                SUM(CASE
                                    WHEN o_orderpriority <> 'orderPriority' AND o_orderpriority <> 'orderPriority1' THEN 1
                                    ELSE 0
                                END) AS low_line_count
                            FROM
                                orders
                            JOIN
                                lineitem ON o_orderkey = l_orderkey
                            WHERE
                                l_shipmode = 'mode'
                                AND l_commitdate < l_receiptdate
                                AND l_shipdate < l_commitdate
                                AND l_receiptdate >= DATE '2020-07-03'
                                AND l_receiptdate < DATE '2020-07-03' + INTERVAL '1' YEAR
                            GROUP BY
                                l_shipmode
                            ORDER BY
                                l_shipmode""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of()
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ13() throws SQLException {
        assumeFalse( System.getProperty( "java.version" ).startsWith( "1.8" ) ); // Out of memory error on Java 8
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                COUNT(o_orderkey)
                            FROM
                                customer
                            LEFT OUTER JOIN
                                orders ON c_custkey = o_custkey AND o_comment NOT LIKE 'fast'
                            GROUP BY
                                c_custkey
                            """;
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( new Object[]{ 0L } ) );

                    String queryFull = """
                            SELECT
                                c_count,
                                COUNT(*) AS custdist
                            FROM
                                (
                                    SELECT
                                        c_custkey,
                                        COUNT(o_orderkey) AS c_count
                                    FROM
                                        customer
                                    LEFT OUTER JOIN
                                        orders ON c_custkey = o_custkey AND o_comment NOT LIKE 'fast'
                                    GROUP BY
                                        c_custkey
                                ) AS c_orders
                            GROUP BY
                                c_count
                            ORDER BY
                                custdist DESC,
                                c_count DESC""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( queryFull ),
                            ImmutableList.of( q13_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q13_VIEW AS " + queryFull );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q13_VIEW" ),
                            ImmutableList.of( q13_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q13_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ14() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                100.00 * SUM(
                                    CASE
                                        WHEN p_type LIKE 'Wireless' THEN l_extendedprice * (1 - l_discount)
                                        ELSE 0
                                    END
                                ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
                            FROM
                                lineitem
                            JOIN
                                part ON l_partkey = p_partkey
                            WHERE
                                l_shipdate >= DATE '2020-07-03'
                                AND l_shipdate < DATE '2020-07-03' + INTERVAL '1' MONTH""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q14_TEST_DATA )
                    );

                    statement.executeUpdate(
                            "CREATE VIEW q14_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q14_VIEW" ),
                            ImmutableList.of( q14_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q14_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ15() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String partialQuery = """
                            CREATE VIEW revenue0 AS
                            SELECT
                                l_suppkey AS supplier_no,
                                SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                            FROM
                                lineitem
                            WHERE
                                l_shipdate >= DATE '2020-07-03'
                                AND l_shipdate < DATE '2020-07-03' + INTERVAL '3' MONTH
                            GROUP BY
                                l_suppkey
                            """;
                    statement.executeUpdate( partialQuery );

                    TestHelper.checkResultSet(
                            statement.executeQuery( """
                                    SELECT
                                        s_suppkey,
                                        s_name,
                                        s_address,
                                        s_phone,
                                        total_revenue
                                    FROM
                                        supplier,
                                        revenue0
                                    WHERE
                                        s_suppkey = supplier_no
                                        AND total_revenue = (
                                            SELECT
                                                MAX(total_revenue)
                                            FROM
                                                revenue0
                                        )
                                    ORDER BY
                                        s_suppkey""" ),
                            ImmutableList.of( q15_TEST_DATA )
                    );
                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS revenue0" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ16() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( """
                                    SELECT
                                        p_brand,
                                        p_type,
                                        p_size,
                                        COUNT(DISTINCT ps_suppkey) AS supplier_cnt
                                    FROM
                                        partsupp
                                    JOIN
                                        part ON p_partkey = ps_partkey
                                    WHERE
                                        p_brand <> 'Logitec'
                                        AND p_type NOT LIKE 'Wireless'
                                        AND p_size IN (5, 10, 20, 100, 1, 7, 4, 2)
                                        AND ps_suppkey NOT IN (
                                            SELECT
                                                s_suppkey
                                            FROM
                                                supplier
                                            WHERE
                                                s_comment LIKE 'SupplierComment'
                                        )
                                    GROUP BY
                                        p_brand,
                                        p_type,
                                        p_size
                                    ORDER BY
                                        supplier_cnt DESC,
                                        p_brand,
                                        p_type,
                                        p_size""" ),
                            ImmutableList.of()
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ17() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                SUM(l_extendedprice) / 7.0 AS avg_yearly
                            FROM
                                lineitem
                            JOIN
                                part ON p_partkey = l_partkey
                            WHERE
                                p_brand = 'Logitec'
                                AND p_container = 'container'
                                AND l_quantity > (
                                    SELECT
                                        0.2 * AVG(l_quantity)
                                    FROM
                                        lineitem
                                    WHERE
                                        l_partkey = p_partkey
                                )""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q17_TEST_DATA )
                    );

                    // @formatter:off
                    statement.executeUpdate(
                            "CREATE VIEW q17_VIEW AS " + query);
                    //@formatter:on
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q17_VIEW" ),
                            ImmutableList.of( q17_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q17_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    // Select not possible
    // Caused by: org.hsqldb.HsqlException: data type of expression is not boolean
    // java.lang.RuntimeException: While executing SQL [SELECT "t8"."c_name", "t8"."c_custkey", "t8"."o_orderkey", "t8"."o_orderdate", "t8"."o_totalprice", COALESCE(SUM("t"."l_quantity"), 0) FROM (SELECT "t6"."c_custkey", "t6"."c_name", "t6"."o_orderkey", "t6"."o_totalprice", "t6"."o_orderdate", COALESCE(SUM("t"."l_quantity"), 0) FROM (SELECT "col72" AS "l_orderkey", "col76" AS "l_quantity" FROM "PUBLIC"."tab12") AS "t" INNER JOIN (SELECT "t5"."c_custkey", "t5"."c_name", "t4"."o_orderkey", "t4"."o_custkey", "t4"."o_totalprice", "t4"."o_orderdate", "t2"."col72" AS "l_orderkey" FROM (SELECT "col72" FROM (SELECT "col72", "col76" FROM "PUBLIC"."tab12" GROUP BY "col72", "col76" HAVING "col76" > ?) AS "t1" GROUP BY "col72") AS "t2" INNER JOIN (SELECT "col63" AS "o_orderkey", "col64" AS "o_custkey", "col66" AS "o_totalprice", "col67" AS "o_orderdate" FROM "PUBLIC"."tab11" WHERE ?) AS "t4" ON "t2"."col72" = "t4"."o_orderkey" INNER JOIN (SELECT "col55" AS "c_custkey", "col56" AS "c_name" FROM "PUBLIC"."tab10") AS "t5" ON "t4"."o_custkey" = "t5"."c_custkey") AS "t6" ON "t"."l_orderkey" = "t6"."o_orderkey" GROUP BY "t6"."c_custkey", "t6"."c_name", "t6"."o_orderkey", "t6"."o_totalprice", "t6"."o_orderdate" ORDER BY "t6"."o_totalprice" DESC, "t6"."o_orderdate") AS "t8"] on JDBC sub-schema
    //@Disabled
    @Test
    public void testQ18() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery(
                                    """
                                            SELECT
                                                c_name,
                                                c_custkey,
                                                o_orderkey,
                                                o_orderdate,
                                                o_totalprice,
                                                SUM(l_quantity) AS total_quantity
                                            FROM
                                                customer
                                            JOIN
                                                orders ON c_custkey = o_custkey
                                            JOIN
                                                lineitem ON o_orderkey = l_orderkey
                                            WHERE
                                                o_orderkey IN (
                                                    SELECT
                                                        l_orderkey
                                                    FROM
                                                        lineitem
                                                    GROUP BY
                                                        l_orderkey
                                                    HAVING
                                                        SUM(l_quantity) > 0
                                                )
                                            GROUP BY
                                                c_name,
                                                c_custkey,
                                                o_orderkey,
                                                o_orderdate,
                                                o_totalprice
                                            ORDER BY
                                                o_totalprice DESC,
                                                o_orderdate
                                            LIMIT 100""" ),
                            ImmutableList.of( new Object[]{ "CName", 1, 1, Date.valueOf( "2020-07-03" ), 65.15, 20.15 } )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ19() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    String query = """
                            SELECT
                                SUM(l_extendedprice * (1 - l_discount)) AS revenue
                            FROM
                                lineitem
                            JOIN
                                part ON p_partkey = l_partkey
                            WHERE
                                (
                                    p_partkey = l_partkey
                                    AND p_brand = 'Logitec'
                                    AND p_container IN ('container')
                                    AND l_quantity >= 1
                                    AND p_size BETWEEN 1 AND 6
                                    AND l_shipmode IN ('mode')
                                    AND l_shipinstruct = 'shipingstruct'
                                )
                                OR (
                                    p_partkey = l_partkey
                                    AND p_brand = 'Logitec'
                                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'container')
                                    AND l_quantity >= 1 AND l_quantity <= 10
                                    AND p_size BETWEEN 1 AND 10
                                    AND l_shipmode IN ('mode', 'AIR REG')
                                    AND l_shipinstruct = 'shipingstruct'
                                )
                                OR (
                                    p_partkey = l_partkey
                                    AND p_brand = 'Logitec'
                                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'container')
                                    AND l_quantity >= 1 AND l_quantity <= 10
                                    AND p_size BETWEEN 1 AND 15
                                    AND l_shipmode IN ('mode', 'AIR REG')
                                    AND l_shipinstruct = 'shipingstruct'
                                )""";
                    TestHelper.checkResultSet(
                            statement.executeQuery( query ),
                            ImmutableList.of( q19_TEST_DATA )
                    );

                    statement.executeUpdate(
                            "CREATE VIEW q19_VIEW AS " + query );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q19_VIEW" ),
                            ImmutableList.of( q19_TEST_DATA )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP VIEW IF EXISTS q19_VIEW" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ20() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery(
                                    """
                                            SELECT
                                                s_name,
                                                s_address
                                            FROM
                                                supplier
                                            JOIN
                                                nation ON s_nationkey = n_nationkey
                                            WHERE
                                                s_suppkey IN (
                                                    SELECT
                                                        ps_suppkey
                                                    FROM
                                                        partsupp
                                                    WHERE
                                                        ps_partkey IN (
                                                            SELECT
                                                                p_partkey
                                                            FROM
                                                                part
                                                            WHERE
                                                                p_name LIKE 'Mou%'
                                                        )
                                                        AND ps_availqty > (
                                                            SELECT
                                                                0.5 * SUM(l_quantity)
                                                            FROM
                                                                lineitem
                                                            WHERE
                                                                l_partkey = ps_partkey
                                                                AND l_suppkey = ps_suppkey
                                                                AND l_shipdate >= DATE '1994-01-01'
                                                                AND l_shipdate < DATE '2002-01-01' + INTERVAL '1' YEAR
                                                        )
                                                )
                                                AND n_name = 'Mouse'
                                            ORDER BY
                                                s_name""" ),
                            ImmutableList.of()
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ21() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery(
                                    """
                                            SELECT
                                                s_name,
                                                COUNT(*) AS numwait
                                            FROM
                                                supplier
                                            JOIN
                                                lineitem l1 ON s_suppkey = l1.l_suppkey
                                            JOIN
                                                orders ON o_orderkey = l1.l_orderkey
                                            JOIN
                                                nation ON s_nationkey = n_nationkey
                                            WHERE
                                                o_orderstatus = 'A'
                                                AND l1.l_receiptdate > l1.l_commitdate
                                                AND EXISTS (
                                                    SELECT
                                                        *
                                                    FROM
                                                        lineitem l2
                                                    WHERE
                                                        l2.l_orderkey = l1.l_orderkey
                                                        AND l2.l_suppkey <> l1.l_suppkey
                                                )
                                                AND NOT EXISTS (
                                                    SELECT
                                                        *
                                                    FROM
                                                        lineitem l3
                                                    WHERE
                                                        l3.l_orderkey = l1.l_orderkey
                                                        AND l3.l_suppkey <> l1.l_suppkey
                                                        AND l3.l_receiptdate > l3.l_commitdate
                                                )
                                                AND n_name = 'Switzerland'
                                            GROUP BY
                                                s_name
                                            ORDER BY
                                                numwait DESC,
                                                s_name
                                            LIMIT 100"""
                            ),
                            ImmutableList.of()
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testQ22() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery(
                                    """
                                            SELECT
                                                cntrycode,
                                                COUNT(*) AS numcust,
                                                SUM(c_acctbal) AS totacctbal
                                            FROM
                                                (
                                                    SELECT
                                                        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
                                                        c_acctbal
                                                    FROM
                                                        customer
                                                    WHERE
                                                        SUBSTRING(c_phone FROM 1 FOR 2) IN (?, ?, ?, ?, ?, ?, ?)
                                                        AND c_acctbal > (
                                                            SELECT
                                                                AVG(c_acctbal)
                                                            FROM
                                                                customer
                                                            WHERE
                                                                c_acctbal > 0.00
                                                                AND SUBSTRING(c_phone FROM 1 FOR 2) IN (?, ?, ?, ?, ?, ?, ?)
                                                        )
                                                        AND NOT EXISTS (
                                                            SELECT
                                                                *
                                                            FROM
                                                                orders
                                                            WHERE
                                                                o_custkey = c_custkey
                                                        )
                                                ) AS custsale
                                            GROUP BY
                                                cntrycode
                                            ORDER BY
                                                cntrycode""" ),
                            ImmutableList.of()
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }

}
