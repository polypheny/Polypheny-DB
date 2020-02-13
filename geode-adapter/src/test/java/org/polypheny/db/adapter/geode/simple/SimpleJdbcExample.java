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

package org.polypheny.db.adapter.geode.simple;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;


/**
 * Example of using Geode via JDBC.
 */
@Slf4j
public class SimpleJdbcExample {

    private SimpleJdbcExample() {
    }


    public static void main( String[] args ) throws Exception {

        Properties info = new Properties();
        final String model = "inline:"
                + "{\n"
                + "  version: '1.0',\n"
                + "  schemas: [\n"
                + "     {\n"
                + "       type: 'custom',\n"
                + "       name: 'TEST',\n"
                + "       factory: 'org.polypheny.db.adapter.geode.simple.GeodeSimpleSchemaFactory',\n"
                + "       operand: {\n"
                + "         locatorHost: 'localhost',\n"
                + "         locatorPort: '10334',\n"
                + "         regions: 'BookMaster',\n"
                + "         pdxSerializablePackagePath: 'org.polypheny.db.adapter.geode.domain.*'\n"
                + "       }\n"
                + "     }\n"
                + "  ]\n"
                + "}";
        info.put( "model", model );

        Class.forName( "org.polypheny.db.jdbc.embedded.EmbeddedDriver" );

        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( "SELECT * FROM \"TEST\".\"BookMaster\"" );

        final StringBuilder buf = new StringBuilder();
        while ( resultSet.next() ) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            for ( int i = 1; i <= columnCount; i++ ) {
                buf.append( i > 1 ? "; " : "" )
                        .append( resultSet.getMetaData().getColumnLabel( i ) )
                        .append( "=" )
                        .append( resultSet.getObject( i ) );
            }
            log.info( "Entry: {}", buf.toString() );

            buf.setLength( 0 );
        }

        resultSet.close();
        statement.close();
        connection.close();
    }
}
