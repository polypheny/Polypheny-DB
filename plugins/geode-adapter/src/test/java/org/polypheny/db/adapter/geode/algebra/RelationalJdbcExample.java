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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.adapter.geode.algebra;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;


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
@Slf4j
public class RelationalJdbcExample {


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
                + "       factory: 'org.polypheny.db.adapter.geode.alg.GeodeSchemaFactory',\n"
                + "       operand: {\n"
                + "         locatorHost: 'localhost', \n"
                + "         locatorPort: '10334', \n"
                + "         regions: 'BookMaster,BookCustomer,BookInventory,BookOrder', \n"
                + "         pdxSerializablePackagePath: 'org.polypheny.db.adapter.geode.domain.*' \n"
                + "       }\n"
                + "     }\n"
                + "   ]\n"
                + "}";

        Class.forName( "org.polypheny.db.jdbc.embedded.EmbeddedDriver" );

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
            log.info( "Result entry: {}", buf.toString() );
            buf.setLength( 0 );
        }
        resultSet.close();
        statement.close();
        connection.close();
    }

}
