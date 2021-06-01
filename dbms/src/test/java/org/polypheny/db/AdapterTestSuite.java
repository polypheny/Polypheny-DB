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

package org.polypheny.db;

import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.ADAPTER;
import org.polypheny.db.jdbc.JdbcArrayTest;
import org.polypheny.db.jdbc.JdbcDdlTest;
import org.polypheny.db.jdbc.JdbcDmlTest;
import org.polypheny.db.jdbc.JdbcMetaTest;
import org.polypheny.db.jdbc.JdbcPreparedStatementsTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        JdbcArrayTest.class,
        JdbcDdlTest.class,
        JdbcDmlTest.class,
        JdbcMetaTest.class,
        JdbcPreparedStatementsTest.class
})

@Slf4j
@Category(AdapterTestSuite.class)
public class AdapterTestSuite {

    @Before
    public void before() {
        log.warn( "before" );
    }


    private static void addNewAdapter( ADAPTER adapter ) throws SQLException {
        // Add adapter to Polypheny-DB
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                log.warn( "Removing old adapter" );
                statement.executeUpdate( "ALTER ADAPTERS DROP hsqldb" );
                connection.commit();

                log.warn( "Adding new adapter of type " + adapter.getPath() );
                statement.executeUpdate( "ALTER ADAPTERS ADD hsqldb USING '" + adapter.getPath() + "' WITH '" + new Gson().toJson( adapter.getDefaultSettings() ) + "'" );
                connection.commit();
            }
        }
    }

}
