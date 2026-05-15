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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.adapter.parquet.relational.ParquetRelationalSource;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.prepare.Prepare.PreparedResultImpl;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;

class ParquetNestedJoinPlanningTest {

    private static final String SOURCE_NAME = "parquet_nested_join_test";
    private static final String ORIGIN = "ParquetNestedJoinPlanningTest";
    private static TestHelper helper;
    private static boolean sourceCreated = false;


    private static String table( String tableName ) {
        return SOURCE_NAME + "__" + tableName;
    }


    @BeforeAll
    static void start() throws SQLException {
        helper = TestHelper.getInstance();

        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS ADD \"" + SOURCE_NAME + "\" USING '" + ParquetRelationalSource.NAME + "' AS 'SOURCE' "
                        + "WITH '{method:\"upload\",directory:\"classpath://orders_db\",directoryName:\"classpath://orders_db\",url:\"file:/\",\"" + ParquetRelationalSource.SCHEMA_MODE_SETTING + "\":\"normalized\"}'" );
                connection.commit();
                sourceCreated = true;
            }
        }
    }


    @AfterAll
    static void end() throws SQLException {
        if ( sourceCreated ) {
            try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
                Connection connection = jdbcConnection.getConnection();
                try ( Statement statement = connection.createStatement() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"" + SOURCE_NAME + "\"" );
                    connection.commit();
                }
            }
            helper.checkAllTrxClosed();
        }
    }


    @Test
    void plansFullJoinWithProjectedFilteredParentInput() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT count(*) "
                                    + "FROM " + table( "orders__items" ) + " i "
                                    + "FULL JOIN " + table( "orders__items__discounts" ) + " d "
                                    + "ON d.__polypheny_parent_row_id = i.__polypheny_row_id "
                                    + "WHERE i.quantity = 3" ) ) {
                assertTrue( resultSet.next() );
            }
        }
    }

    @Test
    void appliesLimitAfterFullJoinFilter() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT * "
                                    + "FROM " + table( "orders__items" ) + " i "
                                    + "FULL JOIN " + table( "orders__items__discounts" ) + " d "
                                    + "ON d.__polypheny_parent_row_id = i.__polypheny_row_id "
                                    + "WHERE i.quantity = 3 "
                                    + "LIMIT 2" ) ) {
                int rows = 0;
                while ( resultSet.next() ) {
                    rows++;
                }
                assertEquals( 2, rows );
            }
        }
    }


    @Test
    void plansFullJoinWithLimitAsParquetJoin() {
        String physicalPlan = physicalPlanFor(
                "SELECT * "
                        + "FROM " + table( "orders__items" ) + " i "
                        + "FULL JOIN " + table( "orders__items__discounts" ) + " d "
                        + "ON d.__polypheny_parent_row_id = i.__polypheny_row_id "
                        + "WHERE i.quantity = 3 "
                        + "LIMIT 10" );

        assertTrue( physicalPlan.contains( "ParquetRelJoin" ), physicalPlan );
        assertFalse( physicalPlan.contains( "EnumerableJoin" ), physicalPlan );
    }


    @Test
    void plansLeftJoinWithLimitAsParquetJoin() {
        String physicalPlan = physicalPlanFor(
                "SELECT * "
                        + "FROM " + table( "orders__items" ) + " i "
                        + "LEFT JOIN " + table( "orders__items__discounts" ) + " d "
                        + "ON d.__polypheny_parent_row_id = i.__polypheny_row_id "
                        + "WHERE i.quantity = 3 "
                        + "LIMIT 10" );

        assertTrue( physicalPlan.contains( "ParquetRelJoin" ), physicalPlan );
        assertFalse( physicalPlan.contains( "EnumerableJoin" ), physicalPlan );
    }


    private String physicalPlanFor( String query ) {
        TransactionManager transactionManager = TransactionManagerImpl.getInstance();
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, new QueryAnalyzer(), ORIGIN );
        try {
            QueryContext context = QueryContext.builder()
                    .query( query )
                    .language( QueryLanguage.from( "sql" ) )
                    .isAnalysed( true )
                    .usesCache( true )
                    .origin( ORIGIN )
                    .namespaceId( Catalog.defaultNamespaceId )
                    .batch( -1 )
                    .transactionManager( transactionManager )
                    .transactions( List.of( transaction ) )
                    .build();

            List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( context );
            StringBuilder plans = new StringBuilder();
            for ( ExecutedContext executedContext : executedContexts ) {
                if ( executedContext.getException().isPresent() ) {
                    throw new AssertionError( executedContext.getException().orElseThrow() );
                }
                if ( executedContext.getImplementation().getPreparedResult() instanceof PreparedResultImpl<?> preparedResult ) {
                    plans.append( AlgOptUtil.dumpPlan(
                            "-- Physical Plan",
                            preparedResult.getRootAlg(),
                            ExplainFormat.TEXT,
                            ExplainLevel.EXPPLAN_ATTRIBUTES ) );
                }
                executedContext.getIterator().getAllRowsAndClose();
            }
            transaction.commit();
            return plans.toString();
        } catch ( Exception e ) {
            if ( transaction.isActive() ) {
                transaction.rollback( e.getMessage() );
            }
            throw e;
        }
    }

}
