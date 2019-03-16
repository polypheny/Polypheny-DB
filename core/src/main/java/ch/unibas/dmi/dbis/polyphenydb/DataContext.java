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

package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.advise.SqlAdvisor;
import com.google.common.base.CaseFormat;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;


/**
 * Runtime context allowing access to the tables in a database.
 */
public interface DataContext {

    ParameterExpression ROOT = Expressions.parameter( Modifier.FINAL, DataContext.class, "root" );

    /**
     * Returns a sub-schema with a given name, or null.
     */
    SchemaPlus getRootSchema();

    /**
     * Returns the type factory.
     */
    JavaTypeFactory getTypeFactory();

    /**
     * Returns the query provider.
     */
    QueryProvider getQueryProvider();

    /**
     * Returns a context variable.
     *
     * Supported variables include: "sparkContext", "currentTimestamp", "localTimestamp".
     *
     * @param name Name of variable
     */
    Object get( String name );

    /**
     * Variable that may be asked for in a call to {@link DataContext#get}.
     */
    enum Variable {
        UTC_TIMESTAMP( "utcTimestamp", Long.class ),

        /**
         * The time at which the current statement started executing. In milliseconds after 1970-01-01 00:00:00, UTC. Required.
         */
        CURRENT_TIMESTAMP( "currentTimestamp", Long.class ),

        /**
         * The time at which the current statement started executing. In milliseconds after 1970-01-01 00:00:00, in the time zone of the current
         * statement. Required.
         */
        LOCAL_TIMESTAMP( "localTimestamp", Long.class ),

        /**
         * The Spark engine. Available if Spark is on the class path.
         */
        SPARK_CONTEXT( "sparkContext", Object.class ),

        /**
         * A mutable flag that indicates whether user has requested that the current statement be canceled. Cancellation may not be immediate, but implementations of relational operators should check the flag fairly
         * frequently and cease execution (e.g. by returning end of data).
         */
        CANCEL_FLAG( "cancelFlag", AtomicBoolean.class ),

        /**
         * Query timeout in milliseconds. When no timeout is set, the value is 0 or not present.
         */
        TIMEOUT( "timeout", Long.class ),

        /**
         * Advisor that suggests completion hints for SQL statements.
         */
        SQL_ADVISOR( "sqlAdvisor", SqlAdvisor.class ),

        /**
         * Writer to the standard error (stderr).
         */
        STDERR( "stderr", OutputStream.class ),

        /**
         * Reader on the standard input (stdin).
         */
        STDIN( "stdin", InputStream.class ),

        /**
         * Writer to the standard output (stdout).
         */
        STDOUT( "stdout", OutputStream.class ),

        /**
         * Time zone in which the current statement is executing. Required; defaults to the time zone of the JVM if the connection does not specify a time zone.
         */
        TIME_ZONE( "timeZone", TimeZone.class );

        public final String camelName;
        public final Class clazz;


        Variable( String camelName, Class clazz ) {
            this.camelName = camelName;
            this.clazz = clazz;
            assert camelName.equals( CaseFormat.UPPER_UNDERSCORE.to( CaseFormat.LOWER_CAMEL, name() ) );
        }


        /**
         * Returns the value of this variable in a given data context.
         */
        public <T> T get( DataContext dataContext ) {
            //noinspection unchecked
            return (T) clazz.cast( dataContext.get( camelName ) );
        }
    }
}

