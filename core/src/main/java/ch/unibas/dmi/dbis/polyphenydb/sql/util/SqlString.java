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

package ch.unibas.dmi.dbis.polyphenydb.sql.util;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import com.google.common.collect.ImmutableList;


/**
 * String that represents a kocher SQL statement, expression, or fragment.
 *
 * A SqlString just contains a regular Java string, but the SqlString wrapper indicates that the string has been created carefully guarding against all SQL dialect and injection issues.
 *
 * The easiest way to do build a SqlString is to use a {@link SqlBuilder}.
 */
public class SqlString {

    private final String sql;
    private SqlDialect dialect;
    private ImmutableList<Integer> dynamicParameters;


    /**
     * Creates a SqlString.
     */
    public SqlString( SqlDialect dialect, String sql ) {
        this( dialect, sql, ImmutableList.of() );
    }


    /**
     * Creates a SqlString. The SQL might contain dynamic parameters, dynamicParameters designate the order of the parameters.
     *
     * @param sql text
     * @param dynamicParameters indices
     */
    public SqlString( SqlDialect dialect, String sql, ImmutableList<Integer> dynamicParameters ) {
        this.dialect = dialect;
        this.sql = sql;
        this.dynamicParameters = dynamicParameters;
        assert sql != null : "sql must be NOT null";
        assert dialect != null : "dialect must be NOT null";
    }


    @Override
    public int hashCode() {
        return sql.hashCode();
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof SqlString
                && sql.equals( ((SqlString) obj).sql );
    }


    /**
     * {@inheritDoc}
     *
     * Returns the SQL string.
     *
     * @return SQL string
     * @see #getSql()
     */
    @Override
    public String toString() {
        return sql;
    }


    /**
     * Returns the SQL string.
     *
     * @return SQL string
     */
    public String getSql() {
        return sql;
    }


    /**
     * Returns indices of dynamic parameters.
     *
     * @return indices of dynamic parameters
     */
    public ImmutableList<Integer> getDynamicParameters() {
        return dynamicParameters;
    }


    /**
     * Returns the dialect.
     */
    public SqlDialect getDialect() {
        return dialect;
    }
}

