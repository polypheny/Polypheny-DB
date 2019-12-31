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


import ch.unibas.dmi.dbis.polyphenydb.cql.parser.CqlParserPos;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.Objects;
import java.util.Set;


/**
 * A <code>SqlNode</code> is a SQL parse tree.
 *
 * It may be an {@link CqlOperator operator}, {@link CqlLiteral literal}, {@link CqlIdentifier identifier}, and so forth.
 */
public abstract class CqlNode implements Cloneable {

    public static final CqlNode[] EMPTY_ARRAY = new CqlNode[0];

    protected final CqlParserPos pos;

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    CqlNode( CqlParserPos pos ) {
        this.pos = Objects.requireNonNull( pos );
    }


    /**
     * Creates a copy of a SqlNode.
     */
    public static <E extends CqlNode> E clone( E e ) {
        //noinspection unchecked
        return (E) e.clone( e.pos );
    }

    /**
     * Clones a SqlNode with a different position.
     */
    public abstract CqlNode clone( CqlParserPos pos );

    /**
     * Returns the type of node this is, or {@link CqlKind#OTHER} if it's nothing special.
     *
     * @return a {@link CqlKind} value, never null
     * @see #isA
     */
    public CqlKind getKind() {
        return CqlKind.OTHER;
    }

    /**
     * Returns whether this node is a member of an aggregate category.
     *
     * For example, {@code node.isA(CqlKind.QUERY)} returns {@code true} if the node is a SELECT, INSERT, UPDATE etc.
     *
     * This method is shorthand: {@code node.isA(category)} is always equivalent to {@code node.getKind().belongsTo(category)}.
     *
     * @param category Category
     * @return Whether this node belongs to the given category.
     */
    public final boolean isA( Set<CqlKind> category ) {
        return getKind().belongsTo( category );
    }


    public String toString() {
        return toCqlString().getQuery();
    }


    /**
     * Returns the SQL text of the tree of which this <code>SqlNode</code> is the root.
     *
     * @param forceParens wraps all expressions in parentheses; good for parse test, but false by default.
     *
     * Typical return values are:
     * <ul>
     * <li>'It''s a bird!'</li>
     * <li>NULL</li>
     * <li>12.3</li>
     * <li>DATE '1969-04-29'</li>
     * </ul>
     */
    public SimpleStatement toCqlString( boolean forceParens ) {
        return null;
    }


    public SimpleStatement toCqlString() {
        return toCqlString( false );
    }


    public CqlParserPos getParserPosition() {
        return pos;
    }
}
