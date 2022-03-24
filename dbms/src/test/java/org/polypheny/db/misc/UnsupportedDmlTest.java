/*
 * Copyright 2019-2022 The Polypheny Project
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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.excluded.MonetdbExcluded;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.webui.models.Result;

@Category({ AdapterTestSuite.class, CassandraExcluded.class, FileExcluded.class }) // todo fix error with filter in file
public class UnsupportedDmlTest extends MqlTestTemplate {

    @Test
    public void dmlEnumerableTest() {
        insert( "{\"hi\":3,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": 2}})" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkResultSet(
                        res,
                        ImmutableList.of( new Object[]{ "id_", "{\"hi\":3,\"stock\":5}" } ) ) );
    }


    @Test
    @Category(MonetdbExcluded.class) // todo bug in closing adapter?
    public void dmlEnumerableFilterTest() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": 3}})" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        res,
                        ImmutableList.of(
                                new String[]{ "id_", "{\"hi\":3,\"stock\":6}" },
                                new String[]{ "id_", "{\"hi\":5,\"stock\":3}" }
                        ), true ) );
    }


    @Test
    public void dmlEnumerableTestFilter() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse(
                "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": -3}})"
        );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkUnorderedResultSet( res,
                        ImmutableList.of(
                                new String[]{ "id_", "{\"hi\":3,\"stock\":0}" },
                                new String[]{ "id_", "{\"hi\":3,\"stock\":29}" },
                                new String[]{ "id_", "{\"hi\":5,\"stock\":3}" }
                        ), true ) );

    }


    @Test
    @Ignore // this is only a reverence
    public void ddlNormalTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE emps( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );
                statement.executeUpdate( "INSERT INTO emps VALUES (1,1,'foo'), (2,5,'bar'), (3,7,'foobar')" );

                statement.executeQuery( "SELECT \"tprimary\" FROM \"public\".\"emps\"" );

                statement.executeUpdate( "DROP TABLE emps" );

                connection.commit();

            }
        }
    }


    @Test
    @Ignore // this is only a reference
    public void ddlSqlUpdateTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE emps( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );
                statement.executeUpdate( "INSERT INTO emps VALUES (1,1,'foo'), (2,5,'bar'), (3,7,'foobar')" );

                statement.executeUpdate( "UPDATE \"emps\" SET \"tprimary\" = \"tprimary\" + 8 WHERE \"tprimary\" = 1" );

                statement.executeUpdate( "DROP TABLE emps" );

                connection.commit();

            }
        }
    }


    @Test
    @Ignore // this is only a reference
    public void ddlSqlCountTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE emps( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );
                statement.executeUpdate( "INSERT INTO emps VALUES (1,1,'foo'), (2,5,'bar'), (3,7,'foobar')" );

                statement.executeQuery( "SELECT COUNT(*) FROM emps WHERE \"tprimary\" = 1" );

                statement.executeUpdate( "DROP TABLE emps" );

                connection.commit();

            }
        }
    }

}
