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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

/*
Table and Queries from https://github.com/polypheny/OLTPBench/tree/polypheny/src/com/oltpbenchmark/benchmarks/tpch
 */
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class ComplexViewTest {


    private final static String DROP_TABLES_NATION = "DROP TABLE IF EXISTS nation ";
    private final static String DROP_TABLES_REGION = "DROP TABLE IF EXISTS region ";
    private final static String DROP_TABLES_PART = "DROP TABLE IF EXISTS part ";
    private final static String DROP_TABLES_SUPPLIER = "DROP TABLE IF EXISTS supplier ";
    private final static String DROP_TABLES_PARTSUPP =  "DROP TABLE IF EXISTS partsupp ";
    private final static String DROP_TABLES_ORDERS =  "DROP TABLE IF EXISTS orders ";
    private final static String DROP_TABLES_CUSTOMER = "DROP TABLE IF EXISTS customer ";
    private final static String DROP_TABLES_LINEITEM = "DROP TABLE IF EXISTS lineitem ";

    private final static String NATION_TABLE = "CREATE TABLE nation  ( "
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

    private final static String PART_TABLE = "CREATE TABLE part  ( "
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

    private final static String ORDERS_TABLE = "CREATE TABLE orders  ( "
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

    private final static Object[] q3_TEST_DATA = new Object[]{
            1,
            new BigDecimal( "-960.3725" ),
            Date.valueOf( "2020-07-03" ),
            1 };

    private final static Object[] q4_TEST_DATA = new Object[]{
            "orderPriority",
            1L };

    private final static Object[] q5_TEST_DATA = new Object[]{
            "orderPriority",
            1L };


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    public void initTables(Statement statement) throws SQLException {
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

    public void dropTables(Statement statement) throws SQLException {
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
                dropTables(statement);
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
                    dropTables(statement);

                }
            }
        }
    }


    @Test
    public void testDate() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
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
                    statement.executeUpdate( "DROP VIEW date_VIEW" );
                    dropTables(statement);

                }
            }
        }
    }


    @Test
    public void testDecimal() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
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
                    statement.executeUpdate( "DROP VIEW decimal_VIEW" );
                    dropTables(statement);

                }
            }
        }
    }


    @Test
    public void testDecimalDate() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
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
                    statement.executeUpdate( "DROP VIEW decimalDate_VIEW" );
                    dropTables(statement);
                }
            }
        }
    }


    @Test
    public void testDecimalDateInt() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
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
                    statement.executeUpdate( "DROP VIEW decimalDateInt_VIEW" );
                    dropTables(statement);
                }
            }
        }
    }


    @Test
    public void testQ1() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
                initTables(statement);

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

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW q1_VIEW" );
                    dropTables(statement);
                }
            }
        }
    }


    //java.lang.AssertionError: type mismatch: ref: VARCHAR(55) NOT NULL input: INTEGER NOT NULL
    //new Object for result must be created correctly
    @Ignore
    public void testQ2() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
                initTables( statement );

                try {

                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    +     "s_acctbal, "
                                    +     "s_name, "
                                    +     "n_name, "
                                    +     "p_partkey, "
                                    +     "p_mfgr, "
                                    +     "s_address, "
                                    +     "s_phone, "
                                    +     "s_comment "
                                    + "from "
                                    +     "part, "
                                    +     "supplier, "
                                    +     "partsupp, "
                                    +     "nation, "
                                    +     "region "
                                    + "where "
                                    +     "p_partkey = ps_partkey "
                                    +     "and s_suppkey = ps_suppkey "
                                    +     "and p_size = 5 "
                                    +     "and p_type like 'Wireless' "
                                    +     "and s_nationkey = n_nationkey "
                                    +     "and n_regionkey = r_regionkey "
                                    +     "and r_name = 'Basel' "
                                    +     "and ps_supplycost = ( "
                                    +         "select "
                                    +             "min(ps_supplycost) "
                                    +         "from "
                                    +             "partsupp, "
                                    +             "supplier, "
                                    +             "nation, "
                                    +             "region "
                                    +         "where "
                                    +             "p_partkey = ps_partkey "
                                    +             "and s_suppkey = ps_suppkey "
                                    +             "and s_nationkey = n_nationkey "
                                    +             "and n_regionkey = r_regionkey "
                                    +             "and r_name = 'Basel' "
                                    +     ") "
                                    + "order by "
                                    +     "s_acctbal desc, "
                                    +     "n_name, "
                                    +     "s_name, "
                                    +     "p_partkey "
                                    + "limit 100" ),
                            ImmutableList.of( new Object[]{} )
                    );

                    connection.commit();
                } finally {
                    dropTables(statement);
                }
            }
        }
    }


    @Test
    public void testQ3() throws SQLException {

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                dropTables(statement);
                initTables( statement );

                try {

                    TestHelper.checkResultSet(
                            statement.executeQuery( "select "
                                    +     "l_orderkey, "
                                    +     "sum(l_extendedprice * (1 - l_discount)) as revenue, "
                                    +     "o_orderdate, "
                                    +     "o_shippriority "
                                    + "from "
                                    +     "customer, "
                                    +     "orders, "
                                    +     "lineitem "
                                    + "where "
                                    +     "c_mktsegment = 'CSegment' "
                                    +     "and c_custkey = o_custkey "
                                    +     "and l_orderkey = o_orderkey "
                                    +     "and o_orderdate < date '2020-08-03' "
                                    +     "and l_shipdate > date '2020-06-03' "
                                    + "group by "
                                    +     "l_orderkey, "
                                    +     "o_orderdate, "
                                    +     "o_shippriority "
                                    + "order by "
                                    +     "revenue desc, "
                                    +     "o_orderdate "
                                    + "limit 10" ),
                            ImmutableList.of( q3_TEST_DATA )
                    );
/*
                    statement.executeUpdate( "CREATE VIEW q3_VIEW AS "
                                    + "select "
                                    +     "l_orderkey, "
                                    +     "sum(l_extendedprice * (1 - l_discount)) as revenue, "
                                    +     "o_orderdate, "
                                    +     "o_shippriority "
                                    + "from "
                                    +     "customer, "
                                    +     "orders, "
                                    +     "lineitem "
                                    + "where "
                                    +     "c_mktsegment = 'CSegment' "
                                    +     "and c_custkey = o_custkey "
                                    +     "and l_orderkey = o_orderkey "
                                    +     "and o_orderdate < date '2020-08-03' "
                                    +     "and l_shipdate > date '2020-06-03' "
                                    + "group by "
                                    +     "l_orderkey, "
                                    +     "o_orderdate, "
                                    +     "o_shippriority "
                                    + "order by "
                                    +     "revenue desc, "
                                    +     "o_orderdate "
                                    + "limit 10"  );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM q3_VIEW" ),
                            ImmutableList.of( q1_TEST_DATA )
                    );


 */
                    connection.commit();
                } finally {
                    //statement.executeUpdate( "DROP VIEW q3_VIEW" );
                    dropTables(statement);
                }
            }
        }
    }

}
