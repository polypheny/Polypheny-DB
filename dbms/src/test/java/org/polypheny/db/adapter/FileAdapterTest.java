/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


public class FileAdapterTest {

    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        // noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        //todo check rollbacks
    }

    @Test
    public void testFileStore() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER STORES DROP hsqldb" );//so tables will be created by default on the mm store
                statement.executeUpdate( "ALTER STORES ADD \"mm\" USING 'org.polypheny.db.adapter.file.FileStore' WITH '{}'" );
                statement.executeUpdate( "CREATE TABLE preparedTest (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a)) ON STORE \"mm\"" );

                preparedTest( connection );
                batchTest( connection );

                //check inserts
                statement.executeUpdate( "INSERT INTO preparedTest (a,b) VALUES (1,2),(3,4),(5,null)" );
                //insert only into one column
                statement.executeUpdate( "INSERT INTO preparedTest (a) VALUES (6)" );

                //test conditions
                ResultSet rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE a = 3" );
                TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 3, 4 } ) );
                rs.close();
                //test prepared select
                PreparedStatement preparedStatement = connection.prepareStatement( "SELECT * FROM preparedTest  WHERE a = ?" );
                preparedStatement.setInt( 1, 1 );
                preparedStatement.executeQuery();
                preparedStatement.close();

                rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b = 4" );
                TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 3, 4 } ) );
                rs.close();

                //update
                statement.executeUpdate( "UPDATE preparedTest SET b = 5 WHERE b = 4" );//b=6 where b = 5
                //prepared update
                preparedStatement = connection.prepareStatement( "UPDATE preparedTest SET b = 6 WHERE b = ?" );
                preparedStatement.setInt( 1, 5 );
                preparedStatement.executeUpdate();
                preparedStatement.close();
                //check updated value
                rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b = 6" );
                TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 3, 6 } ) );
                rs.close();

                //is null
                rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b IS NULL ORDER BY a" );
                TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 5, null }, new Object[]{ 6, null } ) );
                rs.close();
                //x = null should always return false
                rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b = NULL" );
                TestHelper.checkResultSet( rs, ImmutableList.of() );
                rs.close();
                //check greater equals and check that prepared and batch inserts work
                rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE a >= 10 ORDER BY a" );
                TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 10, 20 }, new Object[]{ 11, 21 }, new Object[]{ 12, 22 } ) );
                rs.close();

                statement.executeUpdate( "DROP TABLE public.preparedTest" );
                statement.executeUpdate( "ALTER STORES DROP mm" );
                connection.commit();
            }
        }
    }

    private void preparedTest( final Connection connection ) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO preparedTest (a,b) VALUES (?,?)" );
        preparedStatement.setInt( 1, 10 );
        preparedStatement.setInt( 2, 20 );
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    private void batchTest( final Connection connection ) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO preparedTest (a,b) VALUES (?,?)" );
        preparedStatement.setInt( 1, 11 );
        preparedStatement.setInt( 2, 21 );
        preparedStatement.addBatch();
        preparedStatement.clearParameters();
        preparedStatement.setInt( 1, 12 );
        preparedStatement.setInt( 2, 22 );
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

}
