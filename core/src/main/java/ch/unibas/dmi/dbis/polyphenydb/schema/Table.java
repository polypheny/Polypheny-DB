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

package ch.unibas.dmi.dbis.polyphenydb.schema;


import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;


/**
 * Table.
 *
 * The typical way for a table to be created is when Polypheny-DB interrogates a user-defined schema in order to validate names appearing in a SQL query.
 * Polypheny-DB finds the schema by calling {@link Schema#getSubSchema(String)} on the connection's root schema, then gets a table by calling {@link Schema#getTable(String)}.
 *
 * Note that a table does not know its name. It is in fact possible for a table to be used more than once, perhaps under multiple names or under multiple schemas.
 * (Compare with the <a href="http://en.wikipedia.org/wiki/Inode">i-node</a> concept in the UNIX filesystem.)
 *
 * A particular table instance may also implement {@link Wrapper}, to give access to sub-objects.
 *
 * @see TableMacro
 */
public interface Table {

    /**
     * Returns this table's row type.
     *
     * This is a struct type whose fields describe the names and types of the columns in this table.
     *
     * The implementer must use the type factory provided. This ensures that the type is converted into a canonical form; other equal types in the same query will use the same object.
     *
     * @param typeFactory Type factory with which to create the type
     * @return Row type
     */
    RelDataType getRowType( RelDataTypeFactory typeFactory );

    /**
     * Returns a provider of statistics about this table.
     */
    Statistic getStatistic();

    /**
     * Type of table.
     */
    Schema.TableType getJdbcTableType();

    /**
     * Determines whether the given {@code column} has been rolled up.
     */
    boolean isRolledUp( String column );

    /**
     * Determines whether the given rolled up column can be used inside the given aggregate function.
     * You can assume that {@code isRolledUp(column)} is {@code true}.
     *
     * @param column The column name for which {@code isRolledUp} is true
     * @param call The aggregate call
     * @param parent Parent node of {@code call} in the {@link SqlNode} tree
     * @param config Config settings. May be null
     * @return true iff the given aggregate call is valid
     */
    boolean rolledUpColumnValidInsideAgg( String column, SqlCall call, SqlNode parent, PolyphenyDbConnectionConfig config );
}

