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
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Example of using Geode via JDBC.
 *
 * Before using this example, you need to populate Geode, as follows:
 *
 * <code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code>
 *
 * This will create a virtual machine with Geode and the "bookshop" and "zips" test data sets.
 */
public class RelationalJdbcExample {

    protected static final Logger LOGGER = LoggerFactory.getLogger( RelationalJdbcExample.class.getName() );


    private RelationalJdbcExample() {
    }


    public static void main( String[] args ) throws Exception {

        final String geodeModelJson = "inline:"
                + "{\n"
                + "  version: '1.0',\n"
                + "  schemas: [\n"
                + "     {\n"
                + "       type: 'custom',\n"
                + "       name: 'TEST',\n"
                + "       factory: 'ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel.GeodeSchemaFactory',\n"
                + "       operand: {\n"
                + "         locatorHost: 'localhost', \n"
                + "         locatorPort: '10334', \n"
                + "         regions: 'BookMaster,BookCustomer,BookInventory,BookOrder', \n"
                + "         pdxSerializablePackagePath: 'ch.unibas.dmi.dbis.polyphenydb.adapter.geode.domain.*' \n"
                + "       }\n"
                + "     }\n"
                + "   ]\n"
                + "}";

        Class.forName( "ch.unibas.dmi.dbis.polyphenydb.jdbc.embedded.EmbeddedDriver" );

        Properties info = new Properties();
        info.put( "model", geodeModelJson );

        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(
                "SELECT \"b\".\"author\", \"b\".\"retailCost\", \"i\".\"quantityInStock\"\n"
                        + "FROM \"TEST\".\"BookMaster\" AS \"b\" "
                        + " INNER JOIN \"TEST\".\"BookInventory\" AS \"i\""
                        + "  ON \"b\".\"itemNumber\" = \"i\".\"itemNumber\"\n "
                        + "WHERE  \"b\".\"retailCost\" > 0" );

        final StringBuilder buf = new StringBuilder();
        while ( resultSet.next() ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
                buf.append( i > 1 ? "; " : "" ).append( metaData.getColumnLabel( i ) ).append( "=" ).append( resultSet.getObject( i ) );
            }
            LOGGER.info( "Result entry: " + buf.toString() );
            buf.setLength( 0 );
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
