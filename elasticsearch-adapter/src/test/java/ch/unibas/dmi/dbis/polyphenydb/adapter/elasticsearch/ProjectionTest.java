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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.embedded.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.ViewTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.ViewTableMacro;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert.ConnectionFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Checks renaming of fields (also upper, lower cases) during projections
 */
@Ignore
public class ProjectionTest {

    @ClassRule
    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    private static final String NAME = "docs";


    @BeforeClass
    public static void setupInstance() throws Exception {

        final Map<String, String> mappings = ImmutableMap.of( "A", "keyword", "b", "keyword", "cCC", "keyword", "DDd", "keyword" );

        NODE.createIndex( NAME, mappings );

        String doc = "{'A': 'aa', 'b': 'bb', 'cCC': 'cc', 'DDd': 'dd'}".replace( '\'', '"' );
        NODE.insertDocument( NAME, (ObjectNode) NODE.mapper().readTree( doc ) );
    }


    private ConnectionFactory newConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Connection createConnection() throws SQLException {
                final Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
                final SchemaPlus root = connection.unwrap( PolyphenyDbEmbeddedConnection.class ).getRootSchema();

                root.add( "elastic", new ElasticsearchSchema( NODE.restClient(), NODE.mapper(), NAME ) );

                // add Polypheny-DB view programmatically
                final String viewSql = String.format( Locale.ROOT,
                        "select cast(_MAP['A'] AS varchar(2)) AS a,"
                                + " cast(_MAP['b'] AS varchar(2)) AS b, "
                                + " cast(_MAP['cCC'] AS varchar(2)) AS c, "
                                + " cast(_MAP['DDd'] AS varchar(2)) AS d "
                                + " from \"elastic\".\"%s\"", NAME );

                ViewTableMacro macro = ViewTable.viewMacro( root, viewSql, Collections.singletonList( "elastic" ), Arrays.asList( "elastic", "view" ), false );
                root.add( "VIEW", macro );

                return connection;
            }
        };
    }


    @Test
    public void projection() {
        PolyphenyDbAssert.that()
                .with( newConnectionFactory() )
                .query( "select * from view" )
                .returns( "A=aa; B=bb; C=cc; D=dd\n" );

        PolyphenyDbAssert.that()
                .with( newConnectionFactory() )
                .query( "select a, b, c, d from view" )
                .returns( "A=aa; B=bb; C=cc; D=dd\n" );

        PolyphenyDbAssert.that()
                .with( newConnectionFactory() )
                .query( "select d, c, b, a from view" )
                .returns( "D=dd; C=cc; B=bb; A=aa\n" );

        PolyphenyDbAssert.that()
                .with( newConnectionFactory() )
                .query( "select a from view" )
                .returns( "A=aa\n" );

        PolyphenyDbAssert.that()
                .with( newConnectionFactory() )
                .query( "select a, b from view" )
                .returns( "A=aa; B=bb\n" );

        PolyphenyDbAssert.that()
                .with( newConnectionFactory() )
                .query( "select b, a from view" )
                .returns( "B=bb; A=aa\n" );

    }

}

