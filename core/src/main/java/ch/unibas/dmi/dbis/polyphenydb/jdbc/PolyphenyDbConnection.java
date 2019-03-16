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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.calcite.linq4j.QueryProvider;


/**
 * Extension to Polypheny-DB's implementation of {@link java.sql.Connection JDBC connection} allows schemas to be defined dynamically.
 *
 * You can start off with an empty connection (no schemas), define one or two schemas, and start querying them.
 *
 * Since a {@code PolyphenyDbConnection} implements the linq4j {@link QueryProvider} interface, you can use a connection to execute expression trees as queries.
 */
public interface PolyphenyDbConnection extends Connection, QueryProvider {

    /**
     * Returns the root schema.
     *
     * You can define objects (such as relations) in this schema, and also nested schemas.
     *
     * @return Root schema
     */
    SchemaPlus getRootSchema();

    /**
     * Returns the type factory.
     *
     * @return Type factory
     */
    JavaTypeFactory getTypeFactory();

    /**
     * Returns an instance of the connection properties.
     *
     * NOTE: The resulting collection of properties is same collection used by the connection, and is writable, but behavior if you modify the collection is undefined. Some implementations might, for example,
     * see a modified property, but only if you set it before you create a statement. We will remove this method when there are better implementations of stateful connections and configuration.
     *
     * @return properties
     */
    Properties getProperties();

    // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
    void setSchema( String schema ) throws SQLException;

    // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
    String getSchema() throws SQLException;

    PolyphenyDbConnectionConfig config();

    /**
     * Creates a context for preparing a statement for execution.
     */
    Context createPrepareContext();
}
