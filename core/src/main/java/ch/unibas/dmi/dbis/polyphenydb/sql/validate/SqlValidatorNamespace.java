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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.List;


/**
 * A namespace describes the relation returned by a section of a SQL query.
 *
 * For example, in the query <code>SELECT emp.deptno, age FROM emp, dept</code>, the FROM clause forms a namespace consisting of two tables EMP and DEPT, and a row type consisting of the combined columns of those tables.
 *
 * Other examples of namespaces include a table in the from list (the namespace contains the constituent columns) and a sub-query (the namespace contains the columns in the SELECT clause of the sub-query).
 *
 * These various kinds of namespace are implemented by classes {@link IdentifierNamespace} for table names, {@link SelectNamespace} for SELECT queries, {@link SetopNamespace} for UNION, EXCEPT and INTERSECT, and so forth.
 * But if you are looking at a SELECT query and call {@link SqlValidator#getNamespace(ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode)}, you may not get a SelectNamespace. Why? Because the validator is allowed to wrap namespaces in other objects which implement {@link SqlValidatorNamespace}.
 * Your SelectNamespace will be there somewhere, but might be one or two levels deep.  Don't try to cast the namespace or use <code>instanceof</code>; use {@link SqlValidatorNamespace#unwrap(Class)} and {@link SqlValidatorNamespace#isWrapperFor(Class)} instead.
 *
 * @see SqlValidator
 * @see SqlValidatorScope
 */
public interface SqlValidatorNamespace {

    /**
     * Returns the validator.
     *
     * @return validator
     */
    SqlValidator getValidator();

    /**
     * Returns the underlying table, or null if there is none.
     */
    SqlValidatorTable getTable();

    /**
     * Returns the row type of this namespace, which comprises a list of names and types of the output columns. If the scope's type has not yet been derived, derives it.
     *
     * @return Row type of this namespace, never null, always a struct
     */
    RelDataType getRowType();

    /**
     * Returns the type of this namespace.
     *
     * @return Row type converted to struct
     */
    RelDataType getType();

    /**
     * Sets the type of this namespace.
     *
     * Allows the type for the namespace to be explicitly set, but usually is called during {@link #validate(RelDataType)}.
     *
     * Implicitly also sets the row type. If the type is not a struct, then the row type is the type wrapped as a struct with a single column, otherwise the type and row type are the same.
     */
    void setType( RelDataType type );

    /**
     * Returns the row type of this namespace, sans any system columns.
     *
     * @return Row type sans system columns
     */
    RelDataType getRowTypeSansSystemColumns();

    /**
     * Validates this namespace.
     *
     * If the scope has already been validated, does nothing.
     *
     * Please call {@link SqlValidatorImpl#validateNamespace} rather than calling this method directly.
     *
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     */
    void validate( RelDataType targetRowType );

    /**
     * Returns the parse tree node at the root of this namespace.
     *
     * @return parse tree node; null for {@link TableNamespace}
     */
    SqlNode getNode();

    /**
     * Returns the parse tree node that at is at the root of this namespace and includes all decorations. If there are no decorations, returns the same as {@link #getNode()}.
     */
    SqlNode getEnclosingNode();

    /**
     * Looks up a child namespace of a given name.
     *
     * For example, in the query <code>select e.name from emps as e</code>, <code>e</code> is an {@link IdentifierNamespace} which has a child <code>name</code> which is a {@link FieldNamespace}.
     *
     * @param name Name of namespace
     * @return Namespace
     */
    SqlValidatorNamespace lookupChild( String name );

    /**
     * Returns whether this namespace has a field of a given name.
     *
     * @param name Field name
     * @return Whether field exists
     */
    boolean fieldExists( String name );

    /**
     * Returns a list of expressions which are monotonic in this namespace. For example, if the namespace represents a relation ordered by a column called "TIMESTAMP", then the list would contain a {@link SqlIdentifier} called "TIMESTAMP".
     */
    List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs();

    /**
     * Returns whether and how a given column is sorted.
     */
    SqlMonotonicity getMonotonicity( String columnName );

    @Deprecated
        // to be removed before 2.0
    void makeNullable();

    /**
     * Returns this namespace, or a wrapped namespace, cast to a particular class.
     *
     * @param clazz Desired type
     * @return This namespace cast to desired type
     * @throws ClassCastException if no such interface is available
     */
    <T> T unwrap( Class<T> clazz );

    /**
     * Returns whether this namespace implements a given interface, or wraps a class which does.
     *
     * @param clazz Interface
     * @return Whether namespace implements given interface
     */
    boolean isWrapperFor( Class<?> clazz );

    /**
     * If this namespace resolves to another namespace, returns that namespace, following links to the end of the chain.
     *
     * A {@code WITH}) clause defines table names that resolve to queries (the body of the with-item). An {@link IdentifierNamespace} typically resolves to a {@link TableNamespace}.
     *
     * You must not call this method before {@link #validate(RelDataType)} has completed.
     */
    SqlValidatorNamespace resolve();

    /**
     * Returns whether this namespace is capable of giving results of the desired modality. {@code true} means streaming, {@code false} means relational.
     *
     * @param modality Modality
     */
    boolean supportsModality( SqlModality modality );
}

