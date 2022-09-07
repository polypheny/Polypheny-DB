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

package org.polypheny.db.misc;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class ProcedureTest {


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void basicTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                setupTestTable(statement);

                try {
                    // create procedure
                    statement.executeUpdate("CREATE PROCEDURE spNoParam $ insert into proceduretest VALUES(101, 'Harold') $");

                    // create view
                    statement.executeUpdate( "CREATE VIEW v_mytable3_spNoParam as select id, name from proceduretest" );

                    // create trigger
                    statement.executeUpdate( "CREATE TRIGGER public trigger_mytable3_spNoParamVariantA ON v_mytable3_spNoParam  AFTER INSERT $ exec Procedure \"APP.public.spNoParam\"" );
                    statement.executeUpdate( "CREATE TRIGGER public trigger_mytable3_spNoParamVariantB ON v_mytable3_spNoParam  AFTER INSERT $ exec Procedure \"APP.public.spNoParam\" id=>:id" );

                    // invoke trigger
                    statement.executeUpdate( "insert into v_mytable3_spNoParam values(999, '999')");

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM proceduretest" ),
                            ImmutableList.of(
                                    new Object[]{ 101, "Harold" },
                                    new Object[]{ 101, "Harold" } ) );
                } finally {
                    // remove fixtures
                    statement.executeUpdate( "DROP TABLE proceduretest" );
                    statement.executeUpdate( "DROP TRIGGER trigger_proceduretest_spNoParamVariantA" );
                    statement.executeUpdate( "DROP VIEW v_proceduretest_spNoParam" );
                    statement.executeUpdate( "DROP PROCEDURE spNoParam" );
                }
            }
        }
    }

    private void setupTestTable(Statement statement) throws SQLException {
        statement.executeUpdate("CREATE TABLE proceduretest( "
                + "id INTEGER NOT NULL, "
                + "name VARCHAR(20) NULL," +
                "PRIMARY KEY (id)) ");
    }
}
