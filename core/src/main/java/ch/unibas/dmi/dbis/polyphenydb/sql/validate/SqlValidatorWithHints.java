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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import java.util.List;


/**
 * Extends {@link SqlValidator} to allow discovery of useful data such as fully qualified names of sql objects, alternative valid sql objects that can be
 * used in the SQL statement (dubbed as hints)
 */
public interface SqlValidatorWithHints extends SqlValidator {

    /**
     * Looks up completion hints for a syntactically correct SQL statement that has been parsed into an expression tree. (Note this should be called
     * after {@link #validate(ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode)}.
     *
     * @param topNode top of expression tree in which to lookup completion hints
     * @param pos indicates the position in the sql statement we want to get completion hints for. For example, "select a.ename, b.deptno from sales.emp a join sales.dept b "on a.deptno=b.deptno where empno=1"; setting pos to 'Line 1, Column 17' returns all the possible column names that can be selected from sales.dept table setting pos to 'Line 1, Column 31' returns all the possible table names in 'sales' schema
     * @return an array of {@link SqlMoniker} (sql identifiers) that can fill in at the indicated position
     */
    List<SqlMoniker> lookupHints( SqlNode topNode, SqlParserPos pos );

    /**
     * Looks up the fully qualified name for a {@link SqlIdentifier} at a given Parser Position in a parsed expression tree Note: call this only after
     * {@link #validate} has been called.
     *
     * @param topNode top of expression tree in which to lookup the qualified name for the SqlIdentifier
     * @param pos indicates the position of the {@link SqlIdentifier} in the SQL statement we want to get the qualified name for
     * @return a string of the fully qualified name of the {@link SqlIdentifier} if the Parser position represents a valid {@link SqlIdentifier}. Else return an empty string
     */
    SqlMoniker lookupQualifiedName( SqlNode topNode, SqlParserPos pos );
}

