/*
 * Copyright 2019-2021 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.excluded.MonetdbExcluded;

/*
 * Table and Queries from https://github.com/polypheny/OLTPBench/tree/polypheny/src/com/oltpbenchmark/benchmarks/tpch
 */
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
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


    @BeforeClass
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
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "o_orderdate "
                                    + "from "
                                    + "orders " ),
                            ImmutableList.of( date_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW date_VIEW AS "
                            + "select "
                            + "o_orderdate "
                            + "from "
                            + "orders " );
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
    @Category(FileExcluded.class)
    public void testDecimal() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ORDERS_TABLE );
                statement.executeUpdate( ORDERS_TABLE_DATA );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "o_totalprice "
                                    + "from "
                                    + "orders " ),
                            ImmutableList.of( decimal_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW decimal_VIEW AS "
                            + "select "
                            + "o_totalprice "
                            + "from "
                            + "orders " );
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
    @Category(FileExcluded.class)
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
    @Category({ FileExcluded.class })
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
    @Category(FileExcluded.class)
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
    @Category({ FileExcluded.class })
    public void testQ1() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select l_returnflag, l_linestatus, "
                                    + "sum(l_quantity) as sum_qty, "
                                    + "sum(l_extendedprice) as sum_base_price, "
                                    + "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
                                    + "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
                                    + "avg(l_quantity) as avg_qty, "
                                    + "avg(l_extendedprice) as avg_price, "
                                    + "avg(l_discount) as avg_disc, "
                                    + "count(*) as count_order "
                                    + "from lineitem "
                                    + "where l_shipdate <= date '2020-07-03' "
                                    + "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus" ),
                            ImmutableList.of( q1_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q1_VIEW AS "
                            + "select l_returnflag, l_linestatus, "
                            + "sum(l_quantity) as sum_qty, "
                            + "sum(l_extendedprice) as sum_base_price, "
                            + "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
                            + "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
                            + "avg(l_quantity) as avg_qty, "
                            + "avg(l_extendedprice) as avg_price, "
                            + "avg(l_discount) as avg_disc, "
                            + "count(*) as count_order "
                            + "from lineitem "
                            + "where l_shipdate <= date '2020-07-03' "
                            + "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q1_VIEW " ),
                            ImmutableList.of( q1_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE MATERIALIZED VIEW q1_Materialized AS "
                            + "select l_returnflag, l_linestatus, "
                            + "sum(l_quantity) as sum_qty, "
                            + "sum(l_extendedprice) as sum_base_price, "
                            + "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
                            + "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
                            + "avg(l_quantity) as avg_qty, "
                            + "avg(l_extendedprice) as avg_price, "
                            + "avg(l_discount) as avg_disc, "
                            + "count(*) as count_order "
                            + "from lineitem "
                            + "where l_shipdate <= date '2020-07-03' "
                            + "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus "
                            + "FRESHNESS INTERVAL 10 \"min\"" );
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


    // SELECT NOT POSSIBLE
    // java.lang.AssertionError: type mismatch: ref: VARCHAR(55) NOT NULL input: INTEGER NOT NULL
    // new Object for result must be created correctly
    @Ignore
    @Test
    public void testQ2() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "s_acctbal, "
                                    + "s_name, "
                                    + "n_name, "
                                    + "p_partkey, "
                                    + "p_mfgr, "
                                    + "s_address, "
                                    + "s_phone, "
                                    + "s_comment "
                                    + "from "
                                    + "part, "
                                    + "supplier, "
                                    + "partsupp, "
                                    + "nation, "
                                    + "region "
                                    + "where "
                                    + "p_partkey = ps_partkey "
                                    + "and s_suppkey = ps_suppkey "
                                    + "and p_size = 5 "
                                    + "and p_type like 'Wireless' "
                                    + "and s_nationkey = n_nationkey "
                                    + "and n_regionkey = r_regionkey "
                                    + "and r_name = 'Basel' "
                                    + "and ps_supplycost = ( "
                                    + "select "
                                    + "min(ps_supplycost) "
                                    + "from "
                                    + "partsupp, "
                                    + "supplier, "
                                    + "nation, "
                                    + "region "
                                    + "where "
                                    + "p_partkey = ps_partkey "
                                    + "and s_suppkey = ps_suppkey "
                                    + "and s_nationkey = n_nationkey "
                                    + "and n_regionkey = r_regionkey "
                                    + "and r_name = 'Basel' "
                                    + ") "
                                    + "order by "
                                    + "s_acctbal desc, "
                                    + "n_name, "
                                    + "s_name, "
                                    + "p_partkey "
                                    + "limit 100" ),
                            ImmutableList.of( new Object[]{} )
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
    @Category({ FileExcluded.class, MonetdbExcluded.class })
    public void testQ3() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "l_orderkey, "
                                    + "sum(l_extendedprice * (1 - l_discount)) as revenue, "
                                    + "o_orderdate, "
                                    + "o_shippriority "
                                    + "from "
                                    + "customer, "
                                    + "orders, "
                                    + "lineitem "
                                    + "where "
                                    + "c_mktsegment = 'CSegment' "
                                    + "and c_custkey = o_custkey "
                                    + "and l_orderkey = o_orderkey "
                                    + "and o_orderdate < date '2020-08-03' "
                                    + "and l_shipdate > date '2020-06-03' "
                                    + "group by "
                                    + "l_orderkey, "
                                    + "o_orderdate, "
                                    + "o_shippriority "
                                    + "order by "
                                    + "revenue desc, "
                                    + "o_orderdate "
                                    + "limit 10" ),
                            ImmutableList.of( q3_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q3_VIEW AS "
                            + "select "
                            + "l_orderkey, "
                            + "sum(l_extendedprice * (1 - l_discount)) as revenue, "
                            + "o_orderdate, "
                            + "o_shippriority "
                            + "from "
                            + "customer, "
                            + "orders, "
                            + "lineitem "
                            + "where "
                            + "c_mktsegment = 'CSegment' "
                            + "and c_custkey = o_custkey "
                            + "and l_orderkey = o_orderkey "
                            + "and o_orderdate < date '2020-08-03' "
                            + "and l_shipdate > date '2020-06-03' "
                            + "group by "
                            + "l_orderkey, "
                            + "o_orderdate, "
                            + "o_shippriority "
                            + "order by "
                            + "revenue desc, "
                            + "o_orderdate "
                            + "limit 10" );
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
    @Category(FileExcluded.class)
    public void testQ4() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "o_orderpriority, "
                                    + "count(*) as order_count "
                                    + "from "
                                    + "orders "
                                    + "where "
                                    + "o_orderdate >= date '2020-07-03' "
                                    + "and o_orderdate < date '2020-07-03' + interval '3' month "
                                    + "and exists ( "
                                    + "select "
                                    + "* "
                                    + "from "
                                    + "lineitem "
                                    + "where "
                                    + "l_orderkey = o_orderkey "
                                    + "and l_commitdate < l_receiptdate "
                                    + ") "
                                    + "group by "
                                    + "o_orderpriority "
                                    + "order by "
                                    + "o_orderpriority" ),
                            ImmutableList.of( q4_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q4_VIEW AS "
                            + "select "
                            + "o_orderpriority, "
                            + "count(*) as order_count "
                            + "from "
                            + "orders "
                            + "where "
                            + "o_orderdate >= date '2020-07-03' "
                            + "and o_orderdate < date '2020-07-03' + interval '3' month "
                            + "and exists ( "
                            + "select "
                            + "* "
                            + "from "
                            + "lineitem "
                            + "where "
                            + "l_orderkey = o_orderkey "
                            + "and l_commitdate < l_receiptdate "
                            + ") "
                            + "group by "
                            + "o_orderpriority "
                            + "order by "
                            + "o_orderpriority" );
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
    @Category(FileExcluded.class)
    public void testQ6() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "sum(l_extendedprice * l_discount) as revenue "
                                    + "from "
                                    + "lineitem "
                                    + "where "
                                    + "l_shipdate >= date '2020-07-03' "
                                    + "and l_shipdate < date '2020-07-03' + interval '1' year "
                                    + "and l_discount between 20.14 and 20.16 "
                                    + "and l_quantity < 20.20" ),
                            ImmutableList.of( q6_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q6_VIEW AS "
                            + "select "
                            + "sum(l_extendedprice * l_discount) as revenue "
                            + "from "
                            + "lineitem "
                            + "where "
                            + "l_shipdate >= date '2020-07-03' "
                            + "and l_shipdate < date '2020-07-03' + interval '1' year "
                            + "and l_discount between 20.14 and 20.16 "
                            + "and l_quantity < 20.20" );
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
    @Category({ FileExcluded.class, MonetdbExcluded.class })
    public void testQ7() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "supp_nation, "
                                    + "cust_nation, "
                                    + "l_year, "
                                    + "sum(volume) as revenue "
                                    + "from "
                                    + "( "
                                    + "select "
                                    + "n1.n_name as supp_nation, "
                                    + "n2.n_name as cust_nation, "
                                    + "extract(year from l_shipdate) as l_year, "
                                    + "l_extendedprice * (1 - l_discount) as volume "
                                    + "from "
                                    + "supplier, "
                                    + "lineitem, "
                                    + "orders, "
                                    + "customer, "
                                    + "nation n1, "
                                    + "nation n2 "
                                    + "where "
                                    + "s_suppkey = l_suppkey "
                                    + "and o_orderkey = l_orderkey "
                                    + "and c_custkey = o_custkey "
                                    + "and s_nationkey = n1.n_nationkey "
                                    + "and c_nationkey = n2.n_nationkey "
                                    + "and (n1.n_name = 'Switzerland' and n2.n_name = 'Switzerland') "
                                    + "and l_shipdate between date '2020-06-03' and date '2020-08-03' "
                                    + ") as shipping "
                                    + "group by "
                                    + "supp_nation, "
                                    + "cust_nation, "
                                    + "l_year "
                                    + "order by "
                                    + "supp_nation, "
                                    + "cust_nation, "
                                    + "l_year" ),
                            ImmutableList.of( q7_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q7_VIEW AS "
                            + "select "
                            + "supp_nation, "
                            + "cust_nation, "
                            + "l_year, "
                            + "sum(volume) as revenue "
                            + "from "
                            + "( "
                            + "select "
                            + "n1.n_name as supp_nation, "
                            + "n2.n_name as cust_nation, "
                            + "extract(year from l_shipdate) as l_year, "
                            + "l_extendedprice * (1 - l_discount) as volume "
                            + "from "
                            + "supplier, "
                            + "lineitem, "
                            + "orders, "
                            + "customer, "
                            + "nation n1, "
                            + "nation n2 "
                            + "where "
                            + "s_suppkey = l_suppkey "
                            + "and o_orderkey = l_orderkey "
                            + "and c_custkey = o_custkey "
                            + "and s_nationkey = n1.n_nationkey "
                            + "and c_nationkey = n2.n_nationkey "
                            + "and (n1.n_name = 'Switzerland' and n2.n_name = 'Switzerland') "
                            + "and l_shipdate between date '2020-06-03' and date '2020-08-03' "
                            + ") as shipping "
                            + "group by "
                            + "supp_nation, "
                            + "cust_nation, "
                            + "l_year "
                            + "order by "
                            + "supp_nation, "
                            + "cust_nation, "
                            + "l_year" );
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
    @Category({ FileExcluded.class, MonetdbExcluded.class })
    public void testQ8() throws SQLException {
        Assume.assumeFalse( System.getProperty( "java.version" ).startsWith( "1.8" ) ); // Out of memory error on Java 8
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "o_year, "
                                    + "sum(case "
                                    + "when nation = 'Switzerland' then volume "
                                    + "else 0 "
                                    + "end) / sum(volume) as mkt_share "
                                    + "from "
                                    + "( "
                                    + "select "
                                    + "extract(year from o_orderdate) as o_year, "
                                    + "l_extendedprice * (1 - l_discount) as volume, "
                                    + "n2.n_name as nation "
                                    + "from "
                                    + "part, "
                                    + "supplier, "
                                    + "lineitem, "
                                    + "orders, "
                                    + "customer, "
                                    + "nation n1, "
                                    + "nation n2, "
                                    + "region "
                                    + "where "
                                    + "p_partkey = l_partkey "
                                    + "and s_suppkey = l_suppkey "
                                    + "and l_orderkey = o_orderkey "
                                    + "and o_custkey = c_custkey "
                                    + "and c_nationkey = n1.n_nationkey "
                                    + "and n1.n_regionkey = r_regionkey "
                                    + "and r_name = 'Basel' "
                                    + "and s_nationkey = n2.n_nationkey "
                                    + "and o_orderdate between date '2020-06-03' and date '2020-08-03' "
                                    + "and p_type = 'Wireless' "
                                    + ") as all_nations "
                                    + "group by "
                                    + "o_year "
                                    + "order by "
                                    + "o_year" ),
                            ImmutableList.of( q8_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q8_VIEW AS "
                            + "select "
                            + "o_year, "
                            + "sum(case "
                            + "when nation = 'Switzerland' then volume "
                            + "else 0 "
                            + "end) / sum(volume) as mkt_share "
                            + "from "
                            + "( "
                            + "select "
                            + "extract(year from o_orderdate) as o_year, "
                            + "l_extendedprice * (1 - l_discount) as volume, "
                            + "n2.n_name as nation "
                            + "from "
                            + "part, "
                            + "supplier, "
                            + "lineitem, "
                            + "orders, "
                            + "customer, "
                            + "nation n1, "
                            + "nation n2, "
                            + "region "
                            + "where "
                            + "p_partkey = l_partkey "
                            + "and s_suppkey = l_suppkey "
                            + "and l_orderkey = o_orderkey "
                            + "and o_custkey = c_custkey "
                            + "and c_nationkey = n1.n_nationkey "
                            + "and n1.n_regionkey = r_regionkey "
                            + "and r_name = 'Basel' "
                            + "and s_nationkey = n2.n_nationkey "
                            + "and o_orderdate between date '2020-06-03' and date '2020-08-03' "
                            + "and p_type = 'Wireless' "
                            + ") as all_nations "
                            + "group by "
                            + "o_year "
                            + "order by "
                            + "o_year" );
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
    @Category({ FileExcluded.class, MonetdbExcluded.class })
    public void testQ9() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "nation, "
                                    + "o_year, "
                                    + "sum(amount) as sum_profit "
                                    + "from "
                                    + "( "
                                    + "select "
                                    + "n_name as nation, "
                                    + "extract(year from o_orderdate) as o_year, "
                                    + "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount "
                                    + "from "
                                    + "part, "
                                    + "supplier, "
                                    + "lineitem, "
                                    + "partsupp, "
                                    + "orders, "
                                    + "nation "
                                    + "where "
                                    + "s_suppkey = l_suppkey "
                                    + "and ps_suppkey = l_suppkey "
                                    + "and ps_partkey = l_partkey "
                                    + "and p_partkey = l_partkey "
                                    + "and o_orderkey = l_orderkey "
                                    + "and s_nationkey = n_nationkey "
                                    + "and p_name like 'Mouse' "
                                    + ") as profit "
                                    + "group by "
                                    + "nation, "
                                    + "o_year "
                                    + "order by "
                                    + "nation, "
                                    + "o_year desc" ),
                            ImmutableList.of( q9_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q9_VIEW AS "
                            + "select "
                            + "nation, "
                            + "o_year, "
                            + "sum(amount) as sum_profit "
                            + "from "
                            + "( "
                            + "select "
                            + "n_name as nation, "
                            + "extract(year from o_orderdate) as o_year, "
                            + "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount "
                            + "from "
                            + "part, "
                            + "supplier, "
                            + "lineitem, "
                            + "partsupp, "
                            + "orders, "
                            + "nation "
                            + "where "
                            + "s_suppkey = l_suppkey "
                            + "and ps_suppkey = l_suppkey "
                            + "and ps_partkey = l_partkey "
                            + "and p_partkey = l_partkey "
                            + "and o_orderkey = l_orderkey "
                            + "and s_nationkey = n_nationkey "
                            + "and p_name like 'Mouse' "
                            + ") as profit "
                            + "group by "
                            + "nation, "
                            + "o_year "
                            + "order by "
                            + "nation, "
                            + "o_year desc" );
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
    @Category({ FileExcluded.class, MonetdbExcluded.class })
    public void testQ10() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "c_custkey, "
                                    + "c_name, "
                                    + "sum(l_extendedprice * (1 - l_discount)) as revenue, "
                                    + "c_acctbal, "
                                    + "n_name, "
                                    + "c_address, "
                                    + "c_phone, "
                                    + "c_comment "
                                    + "from "
                                    + "customer, "
                                    + "orders, "
                                    + "lineitem, "
                                    + "nation "
                                    + "where "
                                    + "c_custkey = o_custkey "
                                    + "and l_orderkey = o_orderkey "
                                    + "and o_orderdate >= date '2020-07-03' "
                                    + "and o_orderdate < date '2020-07-03' + interval '3' month "
                                    + "and l_returnflag = 'R' "
                                    + "and c_nationkey = n_nationkey "
                                    + "group by "
                                    + "c_custkey, "
                                    + "c_name, "
                                    + "c_acctbal, "
                                    + "c_phone, "
                                    + "n_name, "
                                    + "c_address, "
                                    + "c_comment "
                                    + "order by "
                                    + "revenue desc "
                                    + "limit 20" ),
                            ImmutableList.of( q10_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q10_VIEW AS "
                            + "select "
                            + "c_custkey, "
                            + "c_name, "
                            + "sum(l_extendedprice * (1 - l_discount)) as revenue, "
                            + "c_acctbal, "
                            + "n_name, "
                            + "c_address, "
                            + "c_phone, "
                            + "c_comment "
                            + "from "
                            + "customer, "
                            + "orders, "
                            + "lineitem, "
                            + "nation "
                            + "where "
                            + "c_custkey = o_custkey "
                            + "and l_orderkey = o_orderkey "
                            + "and o_orderdate >= date '2020-07-03' "
                            + "and o_orderdate < date '2020-07-03' + interval '3' month "
                            + "and l_returnflag = 'R' "
                            + "and c_nationkey = n_nationkey "
                            + "group by "
                            + "c_custkey, "
                            + "c_name, "
                            + "c_acctbal, "
                            + "c_phone, "
                            + "n_name, "
                            + "c_address, "
                            + "c_comment "
                            + "order by "
                            + "revenue desc "
                            + "limit 20" );
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


    // SELECT NOT POSSIBLE
    // Not possible to Select java.lang.AssertionError: type mismatch: ref: DECIMAL(19, 2) NOT NULL input: INTEGER NOT NULL
    // renamed value to valueAA
    @Ignore
    @Test
    public void testQ11() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "ps_partkey, "
                                    + "sum(ps_supplycost * ps_availqty) as valueAA "
                                    + "from "
                                    + "partsupp, "
                                    + "supplier, "
                                    + "nation "
                                    + "where "
                                    + "ps_suppkey = s_suppkey "
                                    + "and s_nationkey = n_nationkey "
                                    + "and n_name = 'Switzerland' "
                                    + "group by "
                                    + "ps_partkey having "
                                    + "sum(ps_supplycost * ps_availqty) > ( "
                                    + "select "
                                    + "sum(ps_supplycost * ps_availqty) * 0.00001 "
                                    + "from "
                                    + "partsupp, "
                                    + "supplier, "
                                    + "nation "
                                    + "where "
                                    + "ps_suppkey = s_suppkey "
                                    + "and s_nationkey = n_nationkey "
                                    + "and n_name = 'Switzerland' "
                                    + ") "
                                    + "order by "
                                    + "valueAA desc" ),
                            ImmutableList.of( new Object[]{} )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    // Select not possible
    // Caused by: java.sql.SQLSyntaxErrorException: data type cast needed for parameter or null literal in statement [SELECT "t0"."l_shipmode", COALESCE(SUM(CASE WHEN "t1"."o_orderpriority" = ? OR "t1"."o_orderpriority" = ? THEN ? ELSE ? END), 0) AS "high_line_count", COALESCE(SUM(CASE WHEN "t1"."o_orderpriority" <> ? AND "t1"."o_orderpriority" <> ? THEN ? ELSE ? END), 0) AS "low_line_count"
    // changed and l_shipmode in (?,?) to and l_shipmode = 'mode'
    @Ignore
    @Test
    public void testQ12() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "l_shipmode, "
                                    + "sum(case "
                                    + "when o_orderpriority = 'orderPriority' "
                                    + "or o_orderpriority = 'orderPriority1' "
                                    + "then 1 "
                                    + "else 0 "
                                    + "end) as high_line_count, "
                                    + "sum(case "
                                    + "when o_orderpriority <> 'orderPriority' "
                                    + "and o_orderpriority <> 'orderPriority1' "
                                    + "then 1 "
                                    + "else 0 "
                                    + "end) as low_line_count "
                                    + "from "
                                    + "orders, "
                                    + "lineitem "
                                    + "where "
                                    + "o_orderkey = l_orderkey "
                                    + "and l_shipmode = 'mode' "
                                    + "and l_commitdate < l_receiptdate "
                                    + "and l_shipdate < l_commitdate "
                                    + "and l_receiptdate >= date '2020-07-03'  "
                                    + "and l_receiptdate < date '2020-07-03'  + interval '1' year "
                                    + "group by "
                                    + "l_shipmode "
                                    + "order by "
                                    + "l_shipmode" ),
                            ImmutableList.of( new Object[]{} )
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
        Assume.assumeFalse( System.getProperty( "java.version" ).startsWith( "1.8" ) ); // Out of memory error on Java 8
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "c_count, "
                                    + "count(*) as custdist "
                                    + "from "
                                    + "( "
                                    + "select "
                                    + "c_custkey, "
                                    + "count(o_orderkey) as c_count "
                                    + "from "
                                    + "customer left outer join orders on "
                                    + "c_custkey = o_custkey "
                                    + "and o_comment not like 'fast' "
                                    + "group by "
                                    + "c_custkey "
                                    + ") as c_orders "
                                    + "group by "
                                    + "c_count "
                                    + "order by "
                                    + "custdist desc, "
                                    + "c_count desc" ),
                            ImmutableList.of( q13_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q13_VIEW AS "
                            + "select "
                            + "c_count, "
                            + "count(*) as custdist "
                            + "from "
                            + "( "
                            + "select "
                            + "c_custkey, "
                            + "count(o_orderkey) as c_count "
                            + "from "
                            + "customer left outer join orders on "
                            + "c_custkey = o_custkey "
                            + "and o_comment not like 'fast' "
                            + "group by "
                            + "c_custkey "
                            + ") as c_orders "
                            + "group by "
                            + "c_count "
                            + "order by "
                            + "custdist desc, "
                            + "c_count desc" );
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
    @Category(FileExcluded.class)
    public void testQ14() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "100.00 * sum(case "
                                    + "when p_type like 'Wireless' "
                                    + "then l_extendedprice * (1 - l_discount) "
                                    + "else 0 "
                                    + "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "
                                    + "from "
                                    + "lineitem, "
                                    + "part "
                                    + "where "
                                    + "l_partkey = p_partkey "
                                    + "and l_shipdate >= date '2020-07-03' "
                                    + "and l_shipdate < date '2020-07-03' + interval '1' month" ),
                            ImmutableList.of( q14_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q14_VIEW AS "
                            + "select "
                            + "100.00 * sum(case "
                            + "when p_type like 'Wireless' "
                            + "then l_extendedprice * (1 - l_discount) "
                            + "else 0 "
                            + "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "
                            + "from "
                            + "lineitem, "
                            + "part "
                            + "where "
                            + "l_partkey = p_partkey "
                            + "and l_shipdate >= date '2020-07-03' "
                            + "and l_shipdate < date '2020-07-03' + interval '1' month" );
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


    // changed max(total_revenue) to total_revenue // Not possible in normal to SELECT aggregate within inner query
    @Test
    @Category(FileExcluded.class)
    public void testQ15() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    statement.executeUpdate( "create view revenue0 (supplier_no, total_revenue) as "
                            + "select "
                            + "l_suppkey, "
                            + "sum(l_extendedprice * (1 - l_discount)) "
                            + "from "
                            + "lineitem "
                            + "where "
                            + "l_shipdate >= date '2020-07-03' "
                            + "and l_shipdate < date '2020-07-03' + interval '3' month "
                            + "group by "
                            + "l_suppkey" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "s_suppkey, "
                                    + "s_name, "
                                    + "s_address, "
                                    + "s_phone, "
                                    + "total_revenue "
                                    + "from "
                                    + "supplier, "
                                    + "revenue0 "
                                    + "where "
                                    + "s_suppkey = supplier_no "
                                    + "and total_revenue = ( "
                                    + "select "
                                    + "total_revenue "
                                    + "from "
                                    + "revenue0 "
                                    + ") "
                                    + "order by "
                                    + "s_suppkey" ),
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


    // Select not possible
    // Caused by: org.hsqldb.HsqlException: data type cast needed for parameter or null literal
    @Ignore
    @Test
    public void testQ16() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "p_brand, "
                                    + "p_type, "
                                    + "p_size, "
                                    + "count(distinct ps_suppkey) as supplier_cnt "
                                    + "from "
                                    + "partsupp, "
                                    + "part "
                                    + "where "
                                    + "p_partkey = ps_partkey "
                                    + "and p_brand <> 'Logitec' "
                                    + "and p_type not like 'Wireless' "
                                    + "and p_size in (5, 10, 20, 100, 1, 7, 4, 2)  "
                                    + "and ps_suppkey not in ( "
                                    + "select "
                                    + "s_suppkey "
                                    + "from "
                                    + "supplier "
                                    + "where "
                                    + "s_comment like 'SupplierComment' "
                                    + ") "
                                    + "group by "
                                    + "p_brand, "
                                    + "p_type, "
                                    + "p_size "
                                    + "order by "
                                    + "supplier_cnt desc, "
                                    + "p_brand, "
                                    + "p_type, "
                                    + "p_size" ),
                            ImmutableList.of( new Object[]{} )
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
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "sum(l_extendedprice) / 7.0 as avg_yearly "
                                    + "from "
                                    + "lineitem, "
                                    + "part "
                                    + "where "
                                    + "p_partkey = l_partkey "
                                    + "and p_brand = 'Logitec'  "
                                    + "and p_container = 'container' "
                                    + "and l_quantity > ( "
                                    + "select "
                                    + "0.2 * avg(l_quantity) "
                                    + "from "
                                    + "lineitem "
                                    + "where "
                                    + "l_partkey = p_partkey "
                                    + ")" ),
                            ImmutableList.of( q17_TEST_DATA )
                    );

                    // @formatter:off
                    statement.executeUpdate( "CREATE VIEW q17_VIEW AS "
                            +"select "
                            +     "sum(l_extendedprice) / 7.0 as avg_yearly "
                            + "from "
                            +     "lineitem, "
                            +     "part "
                            + "where "
                            + "p_partkey = l_partkey "
                            + "and p_brand = 'Logitec'  "
                            + "and p_container = 'container' "
                            + "and l_quantity > ( "
                            + "select "
                            + "0.2 * avg(l_quantity) "
                            + "from "
                            + "lineitem "
                            + "where "
                            + "l_partkey = p_partkey "
                            + ")" );
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
    @Ignore
    @Test
    public void testQ18() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "c_name, "
                                    + "c_custkey, "
                                    + "o_orderkey, "
                                    + "o_orderdate, "
                                    + "o_totalprice, "
                                    + "sum(l_quantity) "
                                    + "from "
                                    + "customer, "
                                    + "orders, "
                                    + "lineitem "
                                    + "where "
                                    + "o_orderkey in ( "
                                    + "select "
                                    + "l_orderkey "
                                    + "from "
                                    + "lineitem "
                                    + "group by "
                                    + "l_orderkey having "
                                    + "sum(l_quantity) > 0 "
                                    + ") "
                                    + "and c_custkey = o_custkey "
                                    + "and o_orderkey = l_orderkey "
                                    + "group by "
                                    + "c_name, "
                                    + "c_custkey, "
                                    + "o_orderkey, "
                                    + "o_orderdate, "
                                    + "o_totalprice "
                                    + "order by "
                                    + "o_totalprice desc, "
                                    + "o_orderdate "
                                    + "limit 100" ),
                            ImmutableList.of( new Object[]{} )
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
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select sum(l_extendedprice* (1 - l_discount)) as revenue "
                                    + "from lineitem, part "
                                    + "where (p_partkey = l_partkey and p_brand = 'Logitec' "
                                    + "and p_container in ( 'container')  "
                                    + "and l_quantity >= 1 "
                                    + "and p_size between 1 and 6 "
                                    + "and l_shipmode in ('mode') "
                                    + "and l_shipinstruct = 'shipingstruct') "
                                    + "or ( p_partkey = l_partkey "
                                    + "and p_brand = 'Logitec' "
                                    + "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'container') "
                                    + "and l_quantity >= 1 and l_quantity <=  10 "
                                    + "and p_size between 1 and 10 "
                                    + "and l_shipmode in ('mode', 'AIR REG') "
                                    + "and l_shipinstruct = 'shipingstruct' ) or "
                                    + "( p_partkey = l_partkey "
                                    + "and p_brand = 'Logitec' "
                                    + "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'container') "
                                    + "and l_quantity >= 1 and l_quantity <= 10 "
                                    + "and p_size between 1 and 15 "
                                    + "and l_shipmode in ('mode', 'AIR REG') "
                                    + "and l_shipinstruct = 'shipingstruct' )" ),
                            ImmutableList.of( q19_TEST_DATA )
                    );

                    statement.executeUpdate( "CREATE VIEW q19_VIEW AS "
                            + "select sum(l_extendedprice* (1 - l_discount)) as revenue "
                            + "from lineitem, part "
                            + "where (p_partkey = l_partkey and p_brand = 'Logitec' "
                            + "and p_container in ( 'container')  "
                            + "and l_quantity >= 1 "
                            + "and p_size between 1 and 6 "
                            + "and l_shipmode in ('mode') "
                            + "and l_shipinstruct = 'shipingstruct') "
                            + "or ( p_partkey = l_partkey "
                            + "and p_brand = 'Logitec' "
                            + "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'container') "
                            + "and l_quantity >= 1 and l_quantity <=  10 "
                            + "and p_size between 1 and 10 "
                            + "and l_shipmode in ('mode', 'AIR REG') "
                            + "and l_shipinstruct = 'shipingstruct' ) or "
                            + "( p_partkey = l_partkey "
                            + "and p_brand = 'Logitec' "
                            + "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'container') "
                            + "and l_quantity >= 1 and l_quantity <= 10 "
                            + "and p_size between 1 and 15 "
                            + "and l_shipmode in ('mode', 'AIR REG') "
                            + "and l_shipinstruct = 'shipingstruct' )" );
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


    // SELECT NOT POSSIBLE
    // java.lang.AssertionError: type mismatch: ref: VARCHAR(25) NOT NULL input: INTEGER NOT NULL
    @Ignore
    @Test
    public void testQ20() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "s_name, "
                                    + "s_address "
                                    + "from "
                                    + "supplier, "
                                    + "nation "
                                    + "where "
                                    + "s_suppkey in ( "
                                    + "select "
                                    + "ps_suppkey "
                                    + "from "
                                    + "partsupp "
                                    + "where "
                                    + "ps_partkey in ( "
                                    + "select "
                                    + "p_partkey "
                                    + "from "
                                    + "part "
                                    + "where "
                                    + "p_name like ? "
                                    + ") "
                                    + "and ps_availqty > ( "
                                    + "select "
                                    + "0.5 * sum(l_quantity) "
                                    + "from "
                                    + "lineitem "
                                    + "where "
                                    + "l_partkey = ps_partkey "
                                    + "and l_suppkey = ps_suppkey "
                                    + "and l_shipdate >= date ? "
                                    + "and l_shipdate < date ? + interval '1' year "
                                    + ") "
                                    + ") "
                                    + "and s_nationkey = n_nationkey "
                                    + "and n_name = ? "
                                    + "order by "
                                    + "s_name" ),
                            ImmutableList.of( new Object[]{} )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    // SELECT NOT POSSIBLE
    // java.lang.AssertionError: type mismatch: ref: INTEGER NOT NULL input: VARCHAR(1) NOT NULL
    @Ignore
    @Test
    public void testQ21() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery(
                                    "select s_name, count(*) as numwait "
                                            + "from supplier, lineitem l1, orders, nation "
                                            + "where s_suppkey = l1.l_suppkey "
                                            + "and o_orderkey = l1.l_orderkey "
                                            + "and o_orderstatus = 'A' "
                                            + "and l1.l_receiptdate > l1.l_commitdate "
                                            + "and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) "
                                            + "and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) "
                                            + "and s_nationkey = n_nationkey "
                                            + "and n_name = 'Switzerland' "
                                            + "group by s_name "
                                            + "order by numwait desc, s_name "
                                            + "limit 100"
                            ),
                            ImmutableList.of( new Object[]{} )
                    );

                    connection.commit();
                } finally {
                    connection.rollback();
                    dropTables( statement );
                }
            }
        }
    }


    // SELECT NOT POSSIBLE
    // Caused by: java.sql.SQLException: General error
    @Ignore
    @Test
    public void testQ22() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                initTables( statement );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    + "cntrycode, "
                                    + "count(*) as numcust, "
                                    + "sum(c_acctbal) as totacctbal "
                                    + "from "
                                    + "( "
                                    + "select "
                                    + "substring(c_phone from 1 for 2) as cntrycode, "
                                    + "c_acctbal "
                                    + "from "
                                    + "customer "
                                    + "where "
                                    + "substring(c_phone from 1 for 2) in "
                                    + "(?, ?, ?, ?, ?, ?, ?) "
                                    + "and c_acctbal > ( "
                                    + "select "
                                    + "avg(c_acctbal) "
                                    + "from "
                                    + "customer "
                                    + "where "
                                    + "c_acctbal > 0.00 "
                                    + "and substring(c_phone from 1 for 2) in "
                                    + "(?, ?, ?, ?, ?, ?, ?) "
                                    + ") "
                                    + "and not exists ( "
                                    + "select "
                                    + "* "
                                    + "from "
                                    + "orders "
                                    + "where "
                                    + "o_custkey = c_custkey "
                                    + ") "
                                    + ") as custsale "
                                    + "group by "
                                    + "cntrycode "
                                    + "order by "
                                    + "cntrycode" ),
                            ImmutableList.of( new Object[]{} )
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
