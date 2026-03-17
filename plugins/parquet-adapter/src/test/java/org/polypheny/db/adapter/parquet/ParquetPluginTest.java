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

import com.google.common.collect.Ordering;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.Util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ParquetPluginTest {
    private void checkSql( String sql, String model, Consumer<ResultSet> fn ) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put( "model", jsonPath( model ) );
            connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery( sql );
            fn.accept( resultSet );
        } finally {
            close( connection, statement );
        }
    }
    private String jsonPath( String model ) {
        return resourcePath( model + ".json" );
    }

    private String resourcePath( String path ) {
        return Sources.of( ParquetPluginTest.class.getResource( "/" + path ) ).file().getAbsolutePath();
    }

    private void close( Connection connection, Statement statement ) {
        if ( statement != null ) {
            try {
                statement.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
        if ( connection != null ) {
            try {
                connection.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }

    /**
     * Returns a function that checks the contents of a result set against an expected string.
     */
    private static Consumer<ResultSet> expect( final String... expected ) {
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                ParquetPluginTest.collect( lines, resultSet );
                assertEquals( Arrays.asList( expected ), lines );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        };
    }

    private static void collect( List<String> result, ResultSet resultSet ) throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while ( resultSet.next() ) {
            buf.setLength( 0 );
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for ( int i = 1; i <= n; i++ ) {
                buf.append( sep )
                        .append( resultSet.getMetaData().getColumnLabel( i ) )
                        .append( "=" )
                        .append( resultSet.getString( i ) );
                sep = "; ";
            }
            result.add( Util.toLinux( buf.toString() ) );
        }
    }

    /**
     * Returns a function that checks the contents of a result set against an expected string.
     */
    private static Consumer<ResultSet> expectUnordered( String... expected ) {
        final List<String> expectedLines = Ordering.natural().immutableSortedCopy( Arrays.asList( expected ) );
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                ParquetPluginTest.collect( lines, resultSet );
                Collections.sort( lines );
                assertEquals( expectedLines, lines );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        };
    }

    /**
     * Fluent API to perform test actions.
     */
    private class Fluent {

        private final String model;
        private final String sql;
        private final Consumer<ResultSet> expect;


        Fluent( String model, String sql, Consumer<ResultSet> expect ) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }

        /**
         * Runs the test.
         */
        Fluent ok() {
            try {
                checkSql( sql, model, expect );
                return this;
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        }

        /**
         * Assigns a function to call to test whether output is correct.
         */
        Fluent checking( Consumer<ResultSet> expect ) {
            return new Fluent( model, sql, expect );
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query.
         */
        Fluent returns( String... expectedLines ) {
            return checking( expect( expectedLines ) );
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query,
         * in no particular order.
         */
        Fluent returnsUnordered( String... expectedLines ) {
            return checking( expectUnordered( expectedLines ) );
        }

    }

}
