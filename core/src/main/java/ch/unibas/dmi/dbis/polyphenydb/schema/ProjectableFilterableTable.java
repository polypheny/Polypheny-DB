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


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;


/**
 * Table that can be scanned, optionally applying supplied filter expressions, and projecting a given list of columns, without creating an intermediate relational expression.
 *
 * If you wish to write a table that can apply projects but not filters, simply decline all filters.
 *
 * @see ScannableTable
 * @see FilterableTable
 */
public interface ProjectableFilterableTable extends Table {

    /**
     * Returns an enumerable over the rows in this Table.
     *
     * Each row is represented as an array of its column values.
     *
     * The list of filters is mutable.
     * If the table can implement a particular filter, it should remove that filter from the list.
     * If it cannot implement a filter, it should leave it in the list.
     * Any filters remaining will be implemented by the consuming Polypheny-DB operator.
     *
     * The projects are zero-based.
     *
     * @param root Execution context
     * @param filters Mutable list of filters. The method should keep in the list any filters that it cannot apply.
     * @param projects List of projects. Each is the 0-based ordinal of the column to project.
     * @return Enumerable over all rows that match the accepted filters, returning for each row an array of column values, one value for each ordinal in {@code projects}.
     */
    Enumerable<Object[]> scan( DataContext root, List<RexNode> filters, int[] projects );
}
