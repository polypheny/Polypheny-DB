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

package ch.unibas.dmi.dbis.polyphenydb.cql;


import java.util.Collection;
import java.util.EnumSet;


public enum CqlKind {

    // the basics
    /**
     * Expression not covered by any other {@link CqlKind} value.
     *
     * @see #OTHER_FUNCTION
     */
    OTHER,

    /**
     * SELECT statement or sub-query.
     */
    SELECT,

    /**
     * Identifier
     */
    IDENTIFIER,

    /**
     * A literal.
     */
    LITERAL,

    /**
     * Function that is not a special function.
     *
     * @see #FUNCTION
     */
    OTHER_FUNCTION,

    /**
     * POSITION Function
     */
    POSITION,

    /**
     * INSERT statement
     */
    INSERT,

    /**
     * DELETE statement
     */
    DELETE,

    /**
     * UPDATE statement
     */
    UPDATE,

    /**
     * A dynamic parameter.
     */
    DYNAMIC_PARAM,

    /**
     * ORDER BY clause.
     *
     * @see #DESCENDING
     * @see #NULLS_FIRST
     * @see #NULLS_LAST
     */
    ORDER_BY,

    /**
     * WITH clause.
     */
    WITH,

    /**
     * Item in WITH clause.
     */
    WITH_ITEM,

    /**
     * AS operator
     */
    AS,

    PROCEDURE_CALL;


    /**
     * Category consisting of all DML operators.
     *
     * Consists of:
     * {@link #INSERT},
     * {@link #UPDATE},
     * {@link #DELETE},
     * {@link #PROCEDURE_CALL}.
     *
     * NOTE jvs 1-June-2006: For now we treat procedure calls as DML; this makes it easy for JDBC clients to call execute or executeUpdate and not have to process dummy cursor results.
     * If in the future we support procedures which return results sets, we'll need to refine this.
     */
    public static final EnumSet<CqlKind> DML = EnumSet.of( INSERT, DELETE, UPDATE, PROCEDURE_CALL );


    /*public static final EnumSet<CqlKind> DDL =
            EnumSet.of(  )*/


    /**
     * Returns whether this {@code SqlKind} belongs to a given category.
     *
     * A category is a collection of kinds, not necessarily disjoint. For example, QUERY is { SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY, EXPLICIT_TABLE }.
     *
     * @param category Category
     * @return Whether this kind belongs to the given category
     */
    public final boolean belongsTo( Collection<CqlKind> category ) {
        return category.contains( this );
    }


    @SafeVarargs
    private static <E extends Enum<E>> EnumSet<E> concat( EnumSet<E> set0, EnumSet<E>... sets ) {
        EnumSet<E> set = set0.clone();
        for ( EnumSet<E> s : sets ) {
            set.addAll( s );
        }
        return set;
    }


}
