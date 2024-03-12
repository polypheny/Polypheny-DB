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

package org.polypheny.db.adapter;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings("SqlDialectInspection")
public abstract class AbstractSourceTest {

    @Test
    public void basicTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM auction ORDER BY id" ),
                        ImmutableList.of(
                                new Object[]{ 1, "Atari 2600", "The Atari 2600 is a home video game console developed and produced by Atari, Inc.", Timestamp.valueOf( "2021-02-02 12:11:02" ), Timestamp.valueOf( "2021-03-02 11:11:02" ), 1, 1 } ) );
            }
        }
    }


    @Test
    public void insertTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "INSERT INTO bid VALUES (1, 100, timestamp '2021-01-01 10:11:15', 1, 1 )" );
                statement.executeUpdate( "INSERT INTO bid VALUES (2, 500, timestamp '2021-01-03 10:21:15', 1, 1 )" );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM bid ORDER BY id" ),
                        ImmutableList.of(
                                new Object[]{ 1, 100, Timestamp.valueOf( "2021-01-01 10:11:15" ), 1, 1 },
                                new Object[]{ 2, 500, Timestamp.valueOf( "2021-01-03 10:21:15" ), 1, 1 } ) );
            }
        }
    }


    @Test
    public void updateTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "INSERT INTO picture VALUES ('/a/a101.jpg', 'JPG', 15204, 1 ), ('/b/b009.png', 'PNG', 5260, 3 )" );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM picture ORDER BY filename" ),
                        ImmutableList.of(
                                new Object[]{ "/a/a101.jpg", "JPG", 15204, 1 },
                                new Object[]{ "/b/b009.png", "PNG", 5260, 3 } ) );

                statement.executeUpdate( "UPDATE picture SET type='GIF' WHERE filename = '/b/b009.png'" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM picture ORDER BY filename" ),
                        ImmutableList.of(
                                new Object[]{ "/a/a101.jpg", "JPG", 15204, 1 },
                                new Object[]{ "/b/b009.png", "GIF", 5260, 3 } ) );
            }
        }
    }


    @Test
    public void deleteTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "INSERT INTO category VALUES ( 1, 'Computer' ), ( 2, 'Games' ), ( 3, 'Software' )" );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM category ORDER BY id" ),
                        ImmutableList.of(
                                new Object[]{ 1, "Computer" },
                                new Object[]{ 2, "Games" },
                                new Object[]{ 3, "Software" } ) );

                statement.executeUpdate( "DELETE FROM category WHERE id = 2" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM category ORDER BY id" ),
                        ImmutableList.of(
                                new Object[]{ 1, "Computer" },
                                new Object[]{ 3, "Software" } ) );
            }
        }
    }


    @Test
    public void truncateTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "INSERT INTO public.\"user\" VALUES ( 1, 'mail@test.local', '1234', 'Mueller', 'Hans', 'm', date '1989-03-15', 'Basel', '4002', 'Switzerland' )" );
                statement.executeUpdate( "INSERT INTO public.\"user\" VALUES ( 2, 'reto@abc.ch', 'secret', 'Weber', 'Reto', 'm', date '1991-11-23', 'Liestal', '4410', 'Switzerland' )" );
                statement.executeUpdate( "INSERT INTO public.\"user\" VALUES ( 3, 'alice@wonderland.ch', 'A1ic3', 'Schneider', 'Alice', 'f', date '1993-04-05', 'Binningen', '4053', 'Switzerland' )" );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM public.\"user\" ORDER BY id" ),
                        ImmutableList.of(
                                new Object[]{ 1, "mail@test.local", "1234", "Mueller", "Hans", "m", Date.valueOf( "1989-03-15" ), "Basel", "4002", "Switzerland" },
                                new Object[]{ 2, "reto@abc.ch", "secret", "Weber", "Reto", "m", Date.valueOf( "1991-11-23" ), "Liestal", "4410", "Switzerland" },
                                new Object[]{ 3, "alice@wonderland.ch", "A1ic3", "Schneider", "Alice", "f", Date.valueOf( "1993-04-05" ), "Binningen", "4053", "Switzerland" } ) );

                statement.executeUpdate( "TRUNCATE TABLE public.\"user\"" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM public.\"user\" ORDER BY id" ),
                        ImmutableList.of() );
            }
        }
    }

    // --- Helpers ---


    protected static void executeScript( Connection conn, String fileName ) throws SQLException {
        InputStream file = ClassLoader.getSystemResourceAsStream( fileName );
        // Check if file != null
        if ( file == null ) {
            throw new RuntimeException( "Unable to load schema definition file" );
        }
        Statement statement = conn.createStatement();
        try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
            String line = bf.readLine();
            while ( line != null ) {
                if ( !line.startsWith( "--" ) ) {
                    statement.executeUpdate( line );
                }
                line = bf.readLine();
            }
        } catch ( IOException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        }
    }

}
