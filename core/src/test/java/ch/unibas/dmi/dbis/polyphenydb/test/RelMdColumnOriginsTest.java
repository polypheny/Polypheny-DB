/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbConnection;
import com.google.common.collect.ImmutableMultiset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.junit.Test;


/**
 * Test case for POLYPHENYDB-542.
 */
public class RelMdColumnOriginsTest {

    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-542">[POLYPHENYDB-542] Support for Aggregate with grouping sets in RelMdColumnOrigins</a>.
     */
    @Test
    public void testQueryWithAggregateGroupingSets() throws Exception {
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbConnection polyphenyDbConnection = connection.unwrap( PolyphenyDbConnection.class );

        polyphenyDbConnection.getRootSchema().add( "T1", new TableInRootSchemaTest.SimpleTable() );
        Statement statement = polyphenyDbConnection.createStatement();
        ResultSet resultSet =
                statement.executeQuery( "SELECT TABLE1.ID, TABLE2.ID FROM "
                        + "(SELECT GROUPING(A) AS ID FROM T1 "
                        + "GROUP BY ROLLUP(A,B)) TABLE1 "
                        + "JOIN "
                        + "(SELECT GROUPING(A) AS ID FROM T1 "
                        + "GROUP BY ROLLUP(A,B)) TABLE2 "
                        + "ON TABLE1.ID = TABLE2.ID" );

        final String result1 = "ID=0; ID=0";
        final String result2 = "ID=1; ID=1";
        final ImmutableMultiset<String> expectedResult =
                ImmutableMultiset.<String>builder()
                        .addCopies( result1, 25 )
                        .add( result2 )
                        .build();
        assertThat( PolyphenyDbAssert.toSet( resultSet ), equalTo( expectedResult ) );

        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        assertThat( resultSetMetaData.getColumnName( 1 ), equalTo( "ID" ) );
        assertThat( resultSetMetaData.getTableName( 1 ), nullValue() );
        assertThat( resultSetMetaData.getSchemaName( 1 ), nullValue() );
        assertThat( resultSetMetaData.getColumnName( 2 ), equalTo( "ID" ) );
        assertThat( resultSetMetaData.getTableName( 2 ), nullValue() );
        assertThat( resultSetMetaData.getSchemaName( 2 ), nullValue() );
        resultSet.close();
        statement.close();
        connection.close();
    }
}

