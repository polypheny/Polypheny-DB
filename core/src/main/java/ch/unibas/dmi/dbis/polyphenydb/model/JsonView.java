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

package ch.unibas.dmi.dbis.polyphenydb.model;


import java.util.List;


/**
 * View schema element.
 *
 * Like base class {@link JsonTable}, occurs within {@link JsonMapSchema#tables}.
 *
 * <h2>Modifiable views</h2>
 *
 * A view is modifiable if contains only SELECT, FROM, WHERE (no JOIN, aggregation or sub-queries) and every column:
 *
 * <ul>
 * <li>is specified once in the SELECT clause; or
 * <li>occurs in the WHERE clause with a column = literal predicate; or
 * <li>is nullable.
 * </ul>
 *
 * The second clause allows Polypheny-DB to automatically provide the correct value for hidden columns. It is useful in, say, a multi-tenant environment, where the {@code tenantId} column is hidden, mandatory (NOT NULL), and has a constant value for a particular view.
 *
 * Errors regarding modifiable views:
 *
 * <ul>
 * <li>If a view is marked modifiable: true and is not modifiable, Polypheny-DB throws an error while reading the schema.
 * <li>If you submit an INSERT, UPDATE or UPSERT command to a non-modifiable view, Polypheny-DB throws an error when validating the statement.
 * <li>If a DML statement creates a row that would not appear in the view (for example, a row in female_emps, above, with gender = 'M'), Polypheny-DB throws an error when executing the statement.
 * </ul>
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonView extends JsonTable {

    /**
     * SQL query that is the definition of the view.
     *
     * Must be a string or a list of strings (which are concatenated into a multi-line SQL string, separated by newlines).
     */
    public Object sql;

    /**
     * Schema name(s) to use when resolving query.
     *
     * If not specified, defaults to current schema.
     */
    public List<String> path;

    /**
     * Whether this view should allow INSERT requests.
     *
     * <p>The values have the following meanings:
     * <ul>
     * <li>If true, Polypheny-DB throws an error when validating the schema if the view is not modifiable.
     * <li>If null, Polypheny-DB deduces whether the view is modifiable.
     * <li>If false, Polypheny-DB will not allow inserts.
     * </ul>
     *
     * The default value is {@code null}.
     */
    public Boolean modifiable;


    public void accept( ModelHandler handler ) {
        handler.visit( this );
    }


    @Override
    public String toString() {
        return "JsonView(name=" + name + ")";
    }


    /**
     * Returns the SQL query as a string, concatenating a list of lines if necessary.
     */
    public String getSql() {
        return JsonLattice.toString( sql );
    }
}

